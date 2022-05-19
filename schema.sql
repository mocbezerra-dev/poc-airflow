CREATE TABLE dim_marca(
    id SERIAL PRIMARY KEY, 
    nome VARCHAR(255) UNIQUE
);

CREATE TABLE fato_smartphone(
	id SERIAL PRIMARY KEY, 
	preco NUMERIC(2), 
	url VARCHAR(255) UNIQUE, 
	modelo VARCHAR(255), 
	marca_fk INT,
    FOREIGN KEY (marca_fk) REFERENCES dim_marca (id)
);
