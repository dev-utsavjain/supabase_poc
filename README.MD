1. command to make project


2. command for build and run
go build -o bin/supabase-manager.exe cmd/server/main.go
./bin/supabase-manager.exe

postgresql://postgres.kiubinpfapdllnljdfzp:[YOUR-PASSWORD]@aws-1-us-east-1.pooler.supabase.com:5432/postgres


## Usage

### Prerequisites

1.  **Go**: Make sure you have Go installed.
2.  **Supabase Account**: You need a Supabase account and a personal access token and organization ID.
3.  **Environment Variables**: Create a `.env` file in the root of the project with the following content:

    ```
    SUPABASE_ACCESS_TOKEN=your_supabase_access_token
    SUPABASE_ORGANIZATION_ID=your_supabase_organization_id
    API_KEY=a_secret_api_key_for_your_server
    ```

### Starting the server

To start the server, run the following command:

```bash
go run cmd/server/main.go
```

The server will start on port 8080 by default. You can change the port by setting the `PORT` environment variable.

### Creating a new project

To create a new Supabase project, send a POST request to the `/api/projects` endpoint.

**Example using `curl`:**

```bash
curl -X POST http://localhost:8080/api/projects \
-H "X-API-Key: your-api-key" \
-H "Content-Type: application/json" \
-d '{
  "name": "my-awesome-project",
  "region": "us-east-1"
}'
```

This will initiate the project creation process. The response will contain the project ID.

### Fetching project details

To get details about a specific project, send a GET request to the `/api/projects/:id` endpoint.

**Example using `curl`:**

```bash
curl http://localhost:8080/api/projects/{project-id} \
-H "X-API-Key: your-api-key"
```

To include sensitive information like the service key and database password, add the `include_keys=true` query parameter:

```bash
curl http://localhost:8080/api/projects/{project-id}?include_keys=true \
-H "X-API-Key: your-api-key"
```

### Executing SQL

You can execute SQL queries on your Supabase project's database by sending a POST request to the `/api/projects/:id/schema` endpoint.

**Example using `curl` to create a table:**

```bash
curl -X POST http://localhost:8080/api/projects/{project-id}/schema \
-H "X-API-Key: your-api-key" \
-H "Content-Type: application/json" \
-d '{
  "sql": "CREATE TABLE public.todos (id serial, title text, is_complete boolean default false, created_at timestamptz default now());"
}'
```

This will apply the SQL schema to the specified project.