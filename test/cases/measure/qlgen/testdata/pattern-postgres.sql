-- Neutral pattern expectations. PostgreSQL 17.6, C.UTF-8, UTF8 encoding.
-- The production QL contract uses Unicode simple folding; locale-specific folds
-- are tested by the independent Go oracle rather than inferred from PostgreSQL.
WITH cases(name, value, pattern, escape) AS (VALUES
 ('whole-value', 'Amber-Bird', 'Amber', E'\\'),
 ('substring', 'Amber-Bird', '%amber%', E'\\'),
 ('prefix', 'Amber-Bird', 'Amber%', E'\\'),
 ('suffix', 'Amber-Bird', '%Bird', E'\\'),
 ('cross-separator', 'Amber-Bird', '%er-B%', E'\\'),
 ('one-character', 'a-b', 'a_b', E'\\'),
 ('unicode-character', 'a中b', 'a_b', E'\\'),
 ('newline-character', E'a\nb', 'a_b', E'\\'),
 ('literal-underscore', 'a_b', 'a!_b', '!'),
 ('escaped-underscore-rejects', 'a-b', 'a!_b', '!'),
 ('literal-percent', '20%', '20!%', '!'),
 ('literal-star', 'a*b', 'a*b', E'\\'),
 ('literal-question', 'a?b', 'a?b', E'\\'),
 ('default-escape', E'a\\b', E'a\\\\b', E'\\'),
 ('disabled-escape', E'a\\b', E'a\\b', ''),
 ('empty-pattern', '', '', E'\\'),
 ('empty-percent', '', '%', E'\\'),
 ('empty-underscore', '', '_', E'\\'),
 ('null-value', NULL, '%', E'\\'),
 ('null-pattern', 'Amber-Bird', NULL, E'\\')
)
SELECT json_agg(row_to_json(expected)) FROM (
 SELECT name, value, pattern, escape,
        value LIKE pattern ESCAPE escape AS "like",
        value ILIKE pattern ESCAPE escape AS "ilike"
 FROM cases ORDER BY name
) AS expected;
