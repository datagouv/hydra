-- add comment column to resources_exceptions table

ALTER TABLE resources_exceptions
ADD COLUMN comment VARCHAR(255);
