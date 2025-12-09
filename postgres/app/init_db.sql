CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

INSERT INTO categories (name) VALUES 
    ('Data Engineering'), ('Machine Learning'), ('Web Development'), ('DevOps'),
    ('Cloud Computing'), ('Cybersecurity'), ('Blockchain'), ('IoT'), ('AI Ethics'),
    ('Quantum Computing'), ('Bioinformatics'), ('Fintech'), ('Edtech'), ('Healthtech'),
    ('AR/VR'), ('Game Dev'), ('Embedded Systems'), ('Computer Vision'), ('NLP'),
    ('Time Series'), ('Graph Theory'), ('Cryptography'), ('Networking'), ('OS Concepts'),
    ('Compiler Design'), ('Database Theory'), ('Distributed Systems'), ('Microservices'),
    ('Serverless'), ('Containers'), ('CI/CD'), ('Testing'), ('Project Management'),
    ('Technical Writing'), ('Career Growth'), ('Startups'), ('Freelancing'),
    ('Open Source'), ('Research'), ('Ethical Hacking'), ('UX Design'), ('UI Patterns'),
    ('Frontend'), ('Backend'), ('Mobile Dev'), ('Robotics'), ('Data Science'),
    ('Big Data'), ('Analytics'), ('Business Intelligence')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    join_date TIMESTAMP NOT NULL,
    is_member BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS articles (
    article_id SERIAL PRIMARY KEY,
    author_id INT NOT NULL REFERENCES users(user_id),
    title VARCHAR(200) NOT NULL,
    publish_date TIMESTAMP NOT NULL,
    category_id SMALLINT NOT NULL REFERENCES categories(category_id),
    premium BOOLEAN NOT NULL
);

CREATE OR REPLACE FUNCTION validate_premium_author()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.premium AND NOT EXISTS (
        SELECT 1 FROM users
        WHERE user_id = NEW.author_id AND is_member = true
    ) THEN
        RAISE EXCEPTION 'Premium articles require member authors';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(user_id),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    CONSTRAINT valid_dates CHECK (start_date < end_date),
    CONSTRAINT unique_active_sub EXCLUDE (user_id WITH =)
    WHERE (is_active = true)
);
