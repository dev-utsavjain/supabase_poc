package supabase

import ("time"
	"fmt"
)

// Project represents a Supabase project
type Project struct {
	ID             string    `json:"id"`
	OrganizationID string    `json:"organization_id"`
	Name           string    `json:"name"`
	Region         string    `json:"region"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	
	// Database connection info
	DatabaseHost string `json:"database_host,omitempty"`
	DatabaseURL  string `json:"database_url,omitempty"`
	DBPassword   string `json:"-"` // Never serialize password
	
	// API keys and URLs - these come from different API responses
	ProjectRef string `json:"ref,omitempty"`
	Endpoint   string `json:"endpoint,omitempty"`
	AnonKey    string `json:"anon_key,omitempty"`
	ServiceKey string `json:"service_role_key,omitempty"`
}

// GetProjectURL returns the full project URL
func (p *Project) GetProjectURL() string {
	if p.Endpoint != "" {
		return p.Endpoint
	}
	if p.ProjectRef != "" {
		return "https://" + p.ProjectRef + ".supabase.co"
	}
	return ""
}

// GetDatabaseConnectionString returns PostgreSQL connection string
// GetDatabaseConnectionString returns PostgreSQL connection string
func (p *Project) GetDatabaseConnectionString() string {
	if p.DatabaseURL != "" {
		return p.DatabaseURL
	}

	// Always prefer pooler connection (IPv4 compatible)
	if p.ProjectRef != "" && p.DBPassword != "" && p.Region != "" {
		return fmt.Sprintf(
			"postgresql://postgres.%s:%s@aws-1-%s.pooler.supabase.com:5432/postgres?sslmode=require",
			p.ProjectRef,
			p.DBPassword,
			p.Region,
		)
	}

	
	// Fallback to direct connection (may be IPv6 only)
	if p.ProjectRef != "" && p.DBPassword != "" {
		return fmt.Sprintf(
			"postgresql://postgres:%s@db.%s.supabase.co:5432/postgres?sslmode=require",
			p.DBPassword,
			p.ProjectRef,
		)
	}
	
	return ""
}

// IsReady checks if project is ready for use
func (p *Project) IsReady() bool {
	return p.Status == "ACTIVE_HEALTHY"
}

// MigrationResult represents the result of applying a SQL migration
type MigrationResult struct {
	Success        bool          `json:"success"`
	TablesCreated  []string      `json:"tables_created,omitempty"`
	RowsInserted   int           `json:"rows_inserted,omitempty"`
	ExecutionTime  time.Duration `json:"execution_time"`
	Error          string        `json:"error,omitempty"`
	StatementsRun  int           `json:"statements_run"`
}

// CreateProjectRequest represents the request to create a project
type CreateProjectRequest struct {
	Name   string `json:"name" binding:"required"`
	Region string `json:"region,omitempty"`
}

// ApplySchemaRequest represents the request to apply a schema
type ApplySchemaRequest struct {
	SQL string `json:"sql" binding:"required"`
}

// ErrorResponse represents an API error response
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail contains error information
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

// StoredProject represents a project stored in local database
type StoredProject struct {
	ID             string    `json:"id"`
	ProjectRef     string    `json:"project_ref"`
	ProjectURL     string    `json:"project_url"`
	Region         string    `json:"region"`
	AnonKey        string    `json:"anon_key"`
	ServiceKey     string    `json:"-"` // Sensitive, don't expose in JSON by default
	DBPassword     string    `json:"-"` // Sensitive
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// ToStoredProject converts Project to StoredProject
func (p *Project) ToStoredProject() *StoredProject {
	return &StoredProject{
		ID:          p.ID,
		ProjectRef:  p.ProjectRef,
		ProjectURL:  p.GetProjectURL(),
		Region:      p.Region,
		AnonKey:     p.AnonKey,
		ServiceKey:  p.ServiceKey,
		DBPassword:  p.DBPassword,
		Status:      p.Status,
		CreatedAt:   p.CreatedAt,
		UpdatedAt:   time.Now(),
	}
}