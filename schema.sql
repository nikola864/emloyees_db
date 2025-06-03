CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    position VARCHAR(50) NOT NULL,
    hire_date DATE NOT NULL,
    salary DECIMAL(10, 2) NOT NULL,
    manager_id INTEGER REFERENCES employees(id)
);

CREATE INDEX idx_employees_manager_id ON employees(manager_id);
CREATE INDEX idx_employees_position ON employees(position);
CREATE INDEX idx_employees_hire_date ON employees(hire_date);
CREATE INDEX idx_employees_salary ON employees(salary);

