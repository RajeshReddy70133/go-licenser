package main

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/jedib0t/go-pretty/table"
)

func cassandra(hosts []string, keyspace, folderPath, action, statefile, prescript, postscript, diffscript, tag string, logger *log.Logger) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		logger.Fatalf("Error connecting to Cassandra database: %v", err)
	}
	defer session.Close()

	switch action {
	case "migrate":
		cassandraMigrate(session, folderPath, tag, logger)
	case "clear":
		cassandraClearData(session, logger)
	case "clearall":
		cassandraClearAllData(session, keyspace, logger)
	case "check":
		cassandraCheckScripts(session, folderPath, logger)
	case "snapshot":
		cassandraGenerateMigrationScript(hosts[0], keyspace, folderPath, statefile, logger)
	case "delta":
		if err := cassandraProcessFiles(folderPath, prescript, postscript, diffscript); err != nil {
			logger.Println("Error processing SQL scripts:", err)
		}
	default:
		logger.Println("Unsupported action:", action)
	}
}

func cassandraMigrate(session *gocql.Session, folderPath, tag string, logger *log.Logger) {
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS tbl_version (
			Id UUID PRIMARY KEY,
			Major TEXT,
			Minor TEXT,
			ChangeColumn TEXT,
			Status TEXT,
			HashCode TEXT,
			ExecutionTime TIMESTAMP
		);
	`
	if err := session.Query(createTableQuery).Exec(); err != nil {
		logger.Fatalf("Error creating table: %v", err)
	}

	hashCodes := cassandraRetrieveHashCodes(session)
	logger.Println("Connected to Cassandra database")

	var skippedFiles []string
	tableWriter := table.NewWriter()
	tableWriter.SetOutputMirror(logger.Writer())
	tableWriter.AppendHeader(table.Row{"Id", "Major", "Minor", "Change", "Status", "Hash Code", "Execution Time"})

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Printf("Error accessing file %s: %v", path, err)
			return nil
		}

		if info.IsDir() || !strings.HasSuffix(info.Name(), ".cql") {
			return nil
		}

		versions, fileName, hashKeys, scripts, tags := cassandraGetVersion(path)
		majorValue := strings.Split(filepath.Base(filepath.Dir(path)), "_")[0]

		for i, version := range versions {
			hk := hashKeys[i]
			script := scripts[i]
			scriptTag := tags[i]
			status := ""

			if tag != "" {
				if scriptTag != tag {
					continue
				}
			} else if scriptTag != "" {
				continue
			}

			if cassandraContains(hashCodes, hk) && !strings.HasPrefix(fileName, "c") {
				status = "skipped"
				skippedFiles = append(skippedFiles, fileName)
				logger.Printf("Execution skipped due to matching hash code: %s, change: %s\n", filepath.Base(fileName), version)
			} else {
				cqlScript := cassandraRemoveComments(script)
				statements := strings.Split(cqlScript, ";")
				success := true

				for _, statement := range statements {
					if strings.TrimSpace(statement) != "" {
						if err := session.Query(statement).Exec(); err != nil {
							logger.Printf("Error executing script %s: %v", fileName, err)
							success = false
							break
						}
					}
				}

				if success {
					status = "success"
					logger.Printf("Execution success for script: %s, change: %s\n", filepath.Base(fileName), version)
				} else {
					status = "failed"
					logger.Printf("Execution failed for script: %s, change: %s\n", filepath.Base(fileName), version)
				}
			}

			parts := strings.SplitN(fileName, "_", 2)
			minorValue := parts[0]

			existingRecordID := cassandraCheckExistingRecord(session, majorValue, minorValue, version)
			executionTime := time.Now().UTC()

			if status != "failed" {
				if existingRecordID != "" {
					if err := session.Query(
						"UPDATE tbl_version SET Status=?, HashCode=?, ExecutionTime=? WHERE Id=?",
						status, hk, executionTime, existingRecordID).Exec(); err != nil {
						logger.Printf("Error updating record: %v", err)
					}
				} else {
					id := gocql.TimeUUID()
					if err := session.Query(
						"INSERT INTO tbl_version (Id, Major, Minor, ChangeColumn, Status, HashCode, ExecutionTime) VALUES (?, ?, ?, ?, ?, ?, ?)",
						id, majorValue, minorValue, version, status, hk, executionTime).Exec(); err != nil {
						logger.Printf("Error inserting record: %v", err)
					}

					existingRecordID = id.String()
				}
			}

			tableWriter.AppendRow([]interface{}{existingRecordID, majorValue, minorValue, version, status, hk, executionTime})
			hashCodes = append(hashCodes, hk)
		}
		return nil
	})

	if err != nil {
		logger.Fatalf("Error iterating over files: %v", err)
	}

	tableWriter.Render()
}

func cassandraRetrieveHashCodes(session *gocql.Session) []string {
	var hashCodes []string
	iter := session.Query("SELECT HashCode FROM tbl_version").Iter()
	var hashCode string
	for iter.Scan(&hashCode) {
		hashCodes = append(hashCodes, hashCode)
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("Error retrieving hash codes from the database: %v", err)
		return nil
	}

	return hashCodes
}

func cassandraRemoveComments(cqlScript string) string {
	lines := strings.Split(cqlScript, "\n")
	var cleanedLines []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "--") {
			cleanedLines = append(cleanedLines, line)
		}
	}
	return strings.Join(cleanedLines, "\n")
}

func cassandraGetVersion(filePath string) ([]string, string, []string, []string, []string) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error reading file %s: %v", filePath, err)
	}

	lines := strings.Split(string(content), "\n")
	var versions []string
	var hashKeys []string
	var scripts []string
	var tags []string
	var script string

	for _, line := range lines {
		if strings.HasPrefix(line, "--start version:") {
			parts := strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "--start version:")), ";")
			version := strings.TrimSpace(parts[0])
			tag := ""
			if len(parts) > 1 {
				tag = strings.TrimSpace(strings.Split(parts[1], "=")[1])
			}
			versions = append(versions, version)
			tags = append(tags, tag)
		} else if strings.HasPrefix(line, "--end version:") {
			hashKey := fmt.Sprintf("%x", md5.Sum([]byte(script)))
			hashKeys = append(hashKeys, hashKey)
			scripts = append(scripts, script)
			script = ""
		} else {
			script += line + "\n"
		}
	}

	fileName := filepath.Base(filePath)
	return versions, fileName, hashKeys, scripts, tags
}

func cassandraContains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func cassandraCheckExistingRecord(session *gocql.Session, majorValue, minorValue, version string) string {
	var id gocql.UUID
	if err := session.Query("SELECT Id FROM tbl_version WHERE Major=? AND Minor=? AND ChangeColumn=?", majorValue, minorValue, version).Scan(&id); err != nil {
		return ""
	}
	return id.String()
}

func cassandraClearData(session *gocql.Session, logger *log.Logger) {
	tableName := "tbl_version"
	if err := session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)).Exec(); err != nil {
		logger.Fatalf("Error dropping table %s: %v", tableName, err)
	}

	logger.Println("Dropped table:", tableName)
}

func cassandraClearAllData(session *gocql.Session, keyspace string, logger *log.Logger) {
	iter := session.Query("SELECT table_name FROM system_schema.tables WHERE keyspace_name=?", keyspace).Iter()
	var tableName string
	for iter.Scan(&tableName) {
		if err := session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)).Exec(); err != nil {
			logger.Fatalf("Error dropping table %s: %v", tableName, err)
		}

		logger.Println("Dropped table:", tableName)
	}
	if err := iter.Close(); err != nil {
		logger.Fatalf("Error retrieving tables: %v", err)
	}
}

func cassandraCheckScripts(session *gocql.Session, folderPath string, logger *log.Logger) {
	executedScripts := cassandraExecutedScriptsWithHashes(session)
	folderScripts := make(map[string]map[string]string)
	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".cql") {
			versions, fileName, hashCodes, _, _ := cassandraGetVersion(path)
			folderScripts[fileName] = make(map[string]string)
			for i, version := range versions {
				folderScripts[fileName][version] = hashCodes[i]
			}
		}
		return nil
	})
	if err != nil {
		logger.Fatalf("Error walking through folder: %v", err)
	}

	cassandraPrintMissingScripts(executedScripts, folderScripts, logger)
}

func cassandraExecutedScriptsWithHashes(session *gocql.Session) map[string]map[string]string {
	executedScripts := make(map[string]map[string]string)
	iter := session.Query("SELECT ChangeColumn, HashCode FROM tbl_version").Iter()
	var change, hashCode string
	for iter.Scan(&change, &hashCode) {
		if _, exists := executedScripts[change]; !exists {
			executedScripts[change] = make(map[string]string)
		}
		executedScripts[change]["HashCode"] = hashCode
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("Error retrieving executed scripts and hashes: %v", err)
	}
	return executedScripts
}

func cassandraPrintMissingScripts(executedScripts map[string]map[string]string, folderScripts map[string]map[string]string, logger *log.Logger) {
	for fileName, versions := range folderScripts {
		for version, hashCode := range versions {
			if executedVersion, exists := executedScripts[fileName]; !exists || executedVersion["HashCode"] != hashCode {
				logger.Printf("Change %s (version %s) with hash code %s has not been executed in tbl_version\n", fileName, version, hashCode)
			}
		}
	}
}

func cassandraGetTableNames(session *gocql.Session, keyspace string) ([]string, error) {
	query := "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
	iter := session.Query(query, keyspace).Iter()
	var tableNames []string
	var tableName string
	for iter.Scan(&tableName) {
		tableNames = append(tableNames, tableName)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return tableNames, nil
}

func cassandraGenerateScriptForTable(session *gocql.Session, keyspace, tableName string) (string, error) {
	query := `SELECT column_name, type
              FROM system_schema.columns
              WHERE keyspace_name = ? AND table_name = ?`
	iter := session.Query(query, keyspace, tableName).Iter()

	script := fmt.Sprintf("CREATE TABLE %s (\n", tableName)
	var columnName, columnType string
	for iter.Scan(&columnName, &columnType) {
		script += fmt.Sprintf("    %s %s,\n", columnName, columnType)
	}
	if err := iter.Close(); err != nil {
		return "", err
	}
	script = script[:len(script)-2] + "\n);\n\n"
	return script, nil
}

func cassandraGenerateMigrationScript(host, keyspace, folderPath, statefile string, logger *log.Logger) {
	cluster := gocql.NewCluster(host)
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		logger.Println("Error creating connection session:", err)
		return
	}
	defer session.Close()

	tableNames, err := cassandraGetTableNames(session, keyspace)
	if err != nil {
		logger.Println("Error getting table names:", err)
		return
	}

	var combinedScript string
	for _, tableName := range tableNames {
		script, err := cassandraGenerateScriptForTable(session, keyspace, tableName)
		if err != nil {
			logger.Println("Error generating script for table:", err)
			return
		}
		combinedScript += script
	}

	combinedCQLFilePath := filepath.Join(folderPath, statefile)
	file, err := os.Create(combinedCQLFilePath)
	if err != nil {
		logger.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	_, err = file.WriteString(combinedScript)
	if err != nil {
		logger.Println("Error writing to file:", err)
		return
	}

	logger.Println("Script has been generated and saved to the specified file.")
}

// processFiles handles the core logic of reading the files, finding differences, and saving the output.
func cassandraProcessFiles(folderPath, prescript, postscript, diffscript string) error {
	file1Path := filepath.Join(folderPath, prescript)
	file2Path := filepath.Join(folderPath, postscript)
	outputPath := filepath.Join(folderPath, diffscript)

	if _, err := os.Stat(file1Path); os.IsNotExist(err) {
		return fmt.Errorf("file %s does not exist", prescript)
	}

	if _, err := os.Stat(file2Path); os.IsNotExist(err) {
		return fmt.Errorf("file %s does not exist", postscript)
	}

	file1Content, err := ioutil.ReadFile(file1Path)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", prescript, err)
	}

	file2Content, err := ioutil.ReadFile(file2Path)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", postscript, err)
	}

	statementsFile1 := cassandraSQLStatements(string(file1Content))
	statementsFile2 := cassandraSQLStatements(string(file2Content))

	differences := findDifferences(statementsFile1, statementsFile2)

	if err := saveDifferencesToFile(outputPath, differences); err != nil {
		return fmt.Errorf("error saving differences to file: %w", err)
	}

	fmt.Println("Differences have been saved to", diffscript)
	return nil
}

// cassandraSQLStatements splits the SQL script into individual statements.
func cassandraSQLStatements(content string) []string {
	statements := strings.Split(content, ";")
	var trimmedStatements []string
	for _, stmt := range statements {
		trimmedStmt := strings.TrimSpace(stmt)
		if trimmedStmt != "" {
			trimmedStatements = append(trimmedStatements, trimmedStmt+";")
		}
	}
	return trimmedStatements
}

// findDifferences returns the statements that are in file2 but not in file1.
func findDifferences(file1Statements, file2Statements []string) []string {
	file1Set := make(map[string]struct{})
	for _, stmt := range file1Statements {
		file1Set[stmt] = struct{}{}
	}

	var differences []string
	for _, stmt := range file2Statements {
		if _, found := file1Set[stmt]; !found {
			differences = append(differences, stmt)
		}
	}
	return differences
}

// saveDifferencesToFile saves the differences to a file.
func saveDifferencesToFile(filename string, differences []string) error {
	content := strings.Join(differences, "\n\n")
	return ioutil.WriteFile(filename, []byte(content), 0644)
}
