INSERT INTO movie_ratings
WITH movies_cte AS (
    SELECT movieId, imdbId, title
    FROM movies
)
SELECT
    m.movieId as movie_id,
    m.imdbId as imdb_id,
    m.title as movie_title,
    AVG(r.rating) as avg_rating,
    COUNT(r.rating) as num_ratings,
    '{{ts}}' as logical_date
FROM movies_cte m
LEFT JOIN ratings r ON r.movieId = m.movieId
GROUP BY 1, 2, 3
