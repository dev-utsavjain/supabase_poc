package storage

import (
	"database/sql"
	"fmt"
	"time"

	_"modernc.org/sqlite" // Pure Go SQLite - works without CGO
	"supabase-manager/internal/supabase"
)

// SQLiteStorage implements credential storage using SQLite
type SQLiteStorage struct {
	db *sql.DB
}

// NewSQLiteStorage creates a new SQLite storage instance
func NewSQLiteStorage(dbPath string) (*SQLiteStorage, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	storage := &SQLiteStorage{db: db}

	// Initialize schema
	if err := storage.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return storage, nil
}

// initSchema creates the necessary tables
func (s *SQLiteStorage) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS projects (
		id TEXT PRIMARY KEY,
		project_ref TEXT UNIQUE NOT NULL,
		project_url TEXT NOT NULL,
		region TEXT NOT NULL DEFAULT 'us-east-1',
		anon_key TEXT NOT NULL,
		service_key TEXT NOT NULL,
		db_password TEXT NOT NULL,
		status TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_projects_ref ON projects(project_ref);
	CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
	CREATE INDEX IF NOT EXISTS idx_projects_created_at ON projects(created_at);
	`

	_, err := s.db.Exec(schema)
	return err
}

// SaveProject stores a project in the database
func (s *SQLiteStorage) SaveProject(project *supabase.StoredProject) error {
	query := `
		INSERT INTO projects (
			id, project_ref, project_url, region, anon_key, service_key, 
			db_password, status, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			project_url = excluded.project_url,
			region = excluded.region,
			anon_key = excluded.anon_key,
			service_key = excluded.service_key,
			db_password = excluded.db_password,
			status = excluded.status,
			updated_at = excluded.updated_at
	`

	_, err := s.db.Exec(
		query,
		project.ID,
		project.ProjectRef,
		project.ProjectURL,
		project.Region,
		project.AnonKey,
		project.ServiceKey,
		project.DBPassword,
		project.Status,
		project.CreatedAt,
		project.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to save project: %w", err)
	}

	return nil
}

// GetProject retrieves a project by ID
func (s *SQLiteStorage) GetProject(id string) (*supabase.StoredProject, error) {
	query := `
		SELECT id, project_ref, project_url, region, anon_key, service_key,
		       db_password, status, created_at, updated_at
		FROM projects
		WHERE id = ?
	`

	var project supabase.StoredProject
	err := s.db.QueryRow(query, id).Scan(
		&project.ID,
		&project.ProjectRef,
		&project.ProjectURL,
		&project.Region,
		&project.AnonKey,
		&project.ServiceKey,
		&project.DBPassword,
		&project.Status,
		&project.CreatedAt,
		&project.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("project not found")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get project: %w", err)
	}

	return &project, nil
}

// GetProjectByRef retrieves a project by project reference
func (s *SQLiteStorage) GetProjectByRef(projectRef string) (*supabase.StoredProject, error) {
	query := `
		SELECT id, project_ref, project_url, region, anon_key, service_key,
		       db_password, status, created_at, updated_at
		FROM projects
		WHERE project_ref = ?
	`

	var project supabase.StoredProject
	err := s.db.QueryRow(query, projectRef).Scan(
		&project.ID,
		&project.ProjectRef,
		&project.ProjectURL,
		&project.Region,
		&project.AnonKey,
		&project.ServiceKey,
		&project.DBPassword,
		&project.Status,
		&project.CreatedAt,
		&project.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("project not found")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get project: %w", err)
	}

	return &project, nil
}

// ListProjects returns all projects
func (s *SQLiteStorage) ListProjects() ([]*supabase.StoredProject, error) {
	query := `
		SELECT id, project_ref, project_url, region, anon_key, service_key,
		       db_password, status, created_at, updated_at
		FROM projects
		ORDER BY created_at DESC
	`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to list projects: %w", err)
	}
	defer rows.Close()

	var projects []*supabase.StoredProject
	for rows.Next() {
		var project supabase.StoredProject
		err := rows.Scan(
			&project.ID,
			&project.ProjectRef,
			&project.ProjectURL,
			&project.Region,
			&project.AnonKey,
			&project.ServiceKey,
			&project.DBPassword,
			&project.Status,
			&project.CreatedAt,
			&project.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan project: %w", err)
		}
		projects = append(projects, &project)
	}

	return projects, nil
}

// DeleteProject removes a project from the database
func (s *SQLiteStorage) DeleteProject(id string) error {
	query := `DELETE FROM projects WHERE id = ?`

	result, err := s.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete project: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("project not found")
	}

	return nil
}

// UpdateProjectStatus updates the status of a project
func (s *SQLiteStorage) UpdateProjectStatus(id, status string) error {
	query := `
		UPDATE projects 
		SET status = ?, updated_at = ?
		WHERE id = ?
	`

	result, err := s.db.Exec(query, status, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("project not found")
	}

	return nil
}

// Close closes the database connection
func (s *SQLiteStorage) Close() error {
	return s.db.Close()
}

// GetStats returns storage statistics
func (s *SQLiteStorage) GetStats() (map[string]interface{}, error) {
	var totalProjects int
	var activeProjects int

	// Total projects
	err := s.db.QueryRow("SELECT COUNT(*) FROM projects").Scan(&totalProjects)
	if err != nil {
		return nil, fmt.Errorf("failed to get total projects: %w", err)
	}

	// Active projects
	err = s.db.QueryRow(
		"SELECT COUNT(*) FROM projects WHERE status = ?",
		"ACTIVE_HEALTHY",
	).Scan(&activeProjects)
	if err != nil {
		return nil, fmt.Errorf("failed to get active projects: %w", err)
	}

	stats := map[string]interface{}{
		"total_projects":  totalProjects,
		"active_projects": activeProjects,
	}

	return stats, nil
}