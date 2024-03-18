ALTER TABLE message ADD COLUMN searchable_string TEXT;
UPDATE message SET searchable_string = '';
-- sqlite does not support ALTER COLUMN, so we can't make it NOT NULL
