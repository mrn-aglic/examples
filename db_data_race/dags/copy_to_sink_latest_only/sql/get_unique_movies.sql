SELECT movieId, imdbId, title
FROM movies
GROUP BY 1, 2, 3
