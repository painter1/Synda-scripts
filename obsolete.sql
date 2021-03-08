-- SELECT filename,dataset_id,status FROM file WHERE
UPDATE file SET status='obsolete' WHERE
  (status='waiting' OR status='error') AND
  dataset_id IN
  ( SELECT [1st].dataset_id FROM
    ( SELECT * FROM dataset ) [1st]
      INNER JOIN
    ( SELECT * FROM dataset ) [2nd]
    ON [1st].path_without_version = [2nd].path_without_version
    WHERE [1st].version < [2nd].version
  )
;
UPDATE dataset SET status='incomplete,obsolete' WHERE status='in-progress' AND
dataset_id IN (SELECT dataset_id FROM file WHERE status='obsolete');
