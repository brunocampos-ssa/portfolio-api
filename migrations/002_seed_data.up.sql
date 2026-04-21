-- Seed data for local development and teaching demos.

-- Users
INSERT INTO users (id, name, email) VALUES
    ('u1', 'Vitalik', 'vitalik@example.com'),
    ('u2', 'Klever Whale', 'whale@example.com'),
    ('u3', 'Multi Chain', 'multi@example.com')
ON CONFLICT (id) DO NOTHING;

-- Wallets
INSERT INTO wallets (id, user_id, blockchain, address) VALUES
    ('w1', 'u1', 'ethereum', '0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe'),
    ('w2', 'u2', 'klever',   'klv1edd0ymfmv9r2mxk7mdtsk4zfeql5cp9vyn7t4y4adq58vp2r9alslfglw8'),
    ('w3', 'u3', 'ethereum', '0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe'),
    ('w4', 'u3', 'klever',   'klv1edd0ymfmv9r2mxk7mdtsk4zfeql5cp9vyn7t4y4adq58vp2r9alslfglw8')
ON CONFLICT (id) DO NOTHING;
