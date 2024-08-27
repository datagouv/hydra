-- add resources_exceptions table

-- create a table with a resource_id column as foreign key to resources.id
CREATE TABLE resources_exceptions (
    id SERIAL PRIMARY KEY,
    resource_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (resource_id) REFERENCES catalog(id)
);
