CREATE TABLE pedidos (
    pedido_id   INTEGER PRIMARY KEY,
    cliente_id  INTEGER,
    dt          DATE NOT NULL,
    receita     NUMERIC(12, 2) NOT NULL,
    custo       NUMERIC(12, 2) NOT NULL
);

INSERT INTO pedidos (pedido_id, cliente_id, dt, receita, custo) VALUES
    (1, 100, '2025-01-15', 500.00, 300.00),
    (2, 101, '2025-03-22', 750.00, 400.00),
    (3, NULL, '2025-06-10', 200.00, 150.00), -- no cliente_id: dropped by the "limpar" transform
    (4, 102, '2026-02-01', 1200.00, 700.00),
    (5, 103, '2026-05-19', 300.00, 180.00);
