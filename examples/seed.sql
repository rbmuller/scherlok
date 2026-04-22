-- Sample data for Scherlok demo
-- Run: docker compose up -d (creates this automatically)

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    plan TEXT NOT NULL DEFAULT 'free',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount NUMERIC(10, 2) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    category TEXT,
    stock INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed users
INSERT INTO users (name, email, plan) VALUES
    ('Alice', 'alice@example.com', 'pro'),
    ('Bob', 'bob@example.com', 'free'),
    ('Carol', 'carol@example.com', 'pro'),
    ('Dave', 'dave@example.com', 'enterprise'),
    ('Eve', 'eve@example.com', 'free'),
    ('Frank', 'frank@example.com', 'pro'),
    ('Grace', 'grace@example.com', 'free'),
    ('Hank', 'hank@example.com', 'free'),
    ('Ivy', 'ivy@example.com', 'pro'),
    ('Jack', 'jack@example.com', 'enterprise');

-- Seed products
INSERT INTO products (name, price, category, stock) VALUES
    ('Widget A', 29.99, 'widgets', 150),
    ('Widget B', 49.99, 'widgets', 80),
    ('Gadget X', 99.99, 'gadgets', 45),
    ('Gadget Y', 149.99, 'gadgets', 20),
    ('Service Plan', 9.99, 'services', 999);

-- Seed orders
INSERT INTO orders (user_id, amount, status) VALUES
    (1, 29.99, 'completed'),
    (1, 99.99, 'completed'),
    (2, 49.99, 'pending'),
    (3, 149.99, 'completed'),
    (4, 29.99, 'completed'),
    (4, 49.99, 'completed'),
    (4, 99.99, 'shipped'),
    (5, 9.99, 'completed'),
    (6, 29.99, 'pending'),
    (7, 149.99, 'completed'),
    (8, 49.99, 'completed'),
    (9, 99.99, 'shipped'),
    (10, 29.99, 'completed'),
    (10, 149.99, 'completed'),
    (10, 9.99, 'completed');
