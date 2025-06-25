CREATE TABLE candidate (
    id UUID PRIMARY KEY,
    full_name VARCHAR(100),
    gender VARCHAR(10),
    age INT,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    nationality VARCHAR(10),
    picture_url TEXT,
    registered_at TIMESTAMP
);

CREATE TABLE position (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE
);

INSERT INTO position (name) 
VALUES 
('Data Analyst'), 
('Frontend Developer'), 
('Backend Developer'), 
('DevOps Engineer'), 
('HR Executive'), 
('Sales Associate'), 
('Project Manager'), 
('QA Engineer'), 
('UI/UX Designer'); 

CREATE TABLE application (
    id SERIAL PRIMARY KEY,
    candidate_id UUID REFERENCES candidate(id) ON DELETE CASCADE,
    position_id INT REFERENCES position(id) ON DELETE SET NULL,
    status VARCHAR(20),
    score INT,
    experience_years INT,
    submitted_at TIMESTAMP DEFAULT NOW()
);
