CREATE TABLE Users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100),
    password VARCHAR(100)
);

CREATE TABLE Tasks (
    task_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    status ENUM('TODO', 'IN_PROGRESS', 'DONE'),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    priority INT,
    created_by INT,
    FOREIGN KEY (created_by) REFERENCES Users(user_id)
);



CREATE TABLE Task_Assign (
    assignment_id INT AUTO_INCREMENT PRIMARY KEY,
    task_id INT,
    user_id INT,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id),
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);



