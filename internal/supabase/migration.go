package supabase

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// MigrationRunner handles SQL migrations on Supabase databases
type MigrationRunner struct {
	project *Project
	db      *sql.DB
}

// NewMigrationRunner creates a new migration runner
func NewMigrationRunner(project *Project) (*MigrationRunner, error) {
	connStr := project.GetDatabaseConnectionString()
	if connStr == "" {
		return nil, fmt.Errorf("no database connection string available")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test connection with timeout
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	// Ping with retry (database might not be immediately available)
	var pingErr error
	for i := 0; i < 3; i++ {
		pingErr = db.Ping()
		if pingErr == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	if pingErr != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database after retries: %w", pingErr)
	}

	return &MigrationRunner{
		project: project,
		db:      db,
	}, nil
}

// Close closes the database connection
func (mr *MigrationRunner) Close() error {
	if mr.db != nil {
		return mr.db.Close()
	}
	return nil
}

// ApplyMigration executes SQL migration on the database
func (mr *MigrationRunner) ApplyMigration(sqlScript string) (*MigrationResult, error) {
	startTime := time.Now()
	result := &MigrationResult{
		Success: false,
	}

	// Validate SQL
	if err := validateSQL(sqlScript); err != nil {
		result.Error = fmt.Sprintf("SQL validation failed: %v", err)
		return result, err
	}

	// Split SQL into individual statements
	statements := splitSQLStatements(sqlScript)
	result.StatementsRun = len(statements)

	// Begin transaction
	tx, err := mr.db.Begin()
	if err != nil {
		result.Error = fmt.Sprintf("failed to begin transaction: %v", err)
		return result, err
	}

	// Ensure rollback on error
	defer func() {
		if !result.Success {
			tx.Rollback()
		}
	}()

	// Execute each statement
	var tablesCreated []string
	var totalRowsInserted int

	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Execute statement
		execResult, err := tx.Exec(stmt)
		if err != nil {
			result.Error = fmt.Sprintf("statement %d failed: %v\nStatement: %s", i+1, err, stmt[:min(len(stmt), 100)])
			return result, fmt.Errorf("failed to execute statement %d: %w", i+1, err)
		}

		// Track rows affected (for INSERT statements)
		if strings.HasPrefix(strings.ToUpper(stmt), "INSERT") {
			rows, _ := execResult.RowsAffected()
			totalRowsInserted += int(rows)
		}

		// Track created tables
		if strings.HasPrefix(strings.ToUpper(stmt), "CREATE TABLE") {
			tableName := extractTableName(stmt)
			if tableName != "" {
				tablesCreated = append(tablesCreated, tableName)
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		result.Error = fmt.Sprintf("failed to commit transaction: %v", err)
		return result, err
	}

	// Success
	result.Success = true
	result.TablesCreated = tablesCreated
	result.RowsInserted = totalRowsInserted
	result.ExecutionTime = time.Since(startTime)

	return result, nil
}

// TestConnection verifies database connectivity
func (mr *MigrationRunner) TestConnection() error {
	return mr.db.Ping()
}

// GetTables returns list of tables in the database
func (mr *MigrationRunner) GetTables() ([]string, error) {
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	rows, err := mr.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// validateSQL performs basic SQL validation
func validateSQL(sql string) error {
	sql = strings.TrimSpace(sql)
	
	if sql == "" {
		return fmt.Errorf("SQL cannot be empty")
	}

	// Check for dangerous operations
	upperSQL := strings.ToUpper(sql)
	dangerous := []string{
		"DROP DATABASE",
		"DROP SCHEMA",
		"TRUNCATE DATABASE",
	}

	for _, danger := range dangerous {
		if strings.Contains(upperSQL, danger) {
			return fmt.Errorf("dangerous operation detected: %s", danger)
		}
	}

	return nil
}

// splitSQLStatements splits SQL script into individual statements
func splitSQLStatements(sql string) []string {
	var statements []string
	var buf bytes.Buffer
	
	inString := false
	inComment := false
	inDollarQuote := false
	dollarQuoteTag := ""
	
	runes := []rune(sql)
	
	for i := 0; i < len(runes); i++ {
		char := runes[i]
		
		// Handle dollar-quoted strings (PostgreSQL specific, used in functions)
		if char == '$' && !inString && !inComment {
			// Check if this is a dollar quote
			tagEnd := i + 1
			for tagEnd < len(runes) && (runes[tagEnd] == '_' || (runes[tagEnd] >= 'a' && runes[tagEnd] <= 'z') || (runes[tagEnd] >= 'A' && runes[tagEnd] <= 'Z') || (runes[tagEnd] >= '0' && runes[tagEnd] <= '9')) {
				tagEnd++
			}
			if tagEnd < len(runes) && runes[tagEnd] == '$' {
				tag := string(runes[i:tagEnd+1])
				if !inDollarQuote {
					inDollarQuote = true
					dollarQuoteTag = tag
				} else if tag == dollarQuoteTag {
					inDollarQuote = false
					dollarQuoteTag = ""
				}
			}
		}
		
		// Handle single-line comments
		if !inString && !inDollarQuote {
			if inComment {
				if char == '\n' {
					inComment = false
				}
			} else {
				if char == '-' && i+1 < len(runes) && runes[i+1] == '-' {
					inComment = true
				}
			}
		}
		
		// Handle single-quoted strings
		if !inComment && !inDollarQuote && char == '\'' {
			if inString {
				// Check for escaped quote
				if i+1 < len(runes) && runes[i+1] == '\'' {
					buf.WriteRune(char)
					i++
					buf.WriteRune(runes[i])
					continue
				}
				inString = false
			} else {
				inString = true
			}
		}
		
		// Handle semicolon (statement separator)
		if char == ';' && !inString && !inComment && !inDollarQuote {
			stmt := strings.TrimSpace(buf.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			buf.Reset()
			continue
		}
		
		buf.WriteRune(char)
	}
	
	// Add remaining statement
	if buf.Len() > 0 {
		stmt := strings.TrimSpace(buf.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	
	return statements
}

// extractTableName extracts table name from CREATE TABLE statement
func extractTableName(stmt string) string {
	upperStmt := strings.ToUpper(strings.TrimSpace(stmt))
	
	if !strings.HasPrefix(upperStmt, "CREATE TABLE") {
		return ""
	}
	
	// Find "CREATE TABLE" and get the next word
	parts := strings.Fields(stmt)
	for i, part := range parts {
		if strings.ToUpper(part) == "TABLE" && i+1 < len(parts) {
			tableName := parts[i+1]
			// Remove IF NOT EXISTS if present
			if strings.ToUpper(tableName) == "IF" {
				if i+4 < len(parts) {
					tableName = parts[i+4]
				} else {
					continue
				}
			}
			// Remove schema prefix if present (e.g., "public.users" -> "users")
			if idx := strings.Index(tableName, "."); idx > 0 {
				tableName = tableName[idx+1:]
			}
			// Remove any parentheses or quotes
			tableName = strings.TrimSuffix(tableName, "(")
			tableName = strings.Trim(tableName, "\"")
			return tableName
		}
	}
	
	return ""
}

// GetRowCount returns the number of rows in a table
func (mr *MigrationRunner) GetRowCount(tableName string) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	
	var count int
	err := mr.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	
	return count, nil
}

// min helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}