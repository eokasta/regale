-- mode="upsert" needs the destination table (and a unique/primary key
-- constraint on `keys`) to already exist — see GenericDriver's docstring.
CREATE TABLE fato_pedidos (
    pedido_id   INTEGER PRIMARY KEY,
    cliente_id  INTEGER,
    dt          DATE NOT NULL,
    receita     NUMERIC(12, 2) NOT NULL,
    custo       NUMERIC(12, 2) NOT NULL,
    margem      NUMERIC(12, 2) NOT NULL
);
