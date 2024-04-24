// UserTasks.js
import React, { useState, useEffect } from 'react';
import axios from 'axios';

function AssignedTasks({ userId }) {
  const [tasks, setTasks] = useState([]);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchUserTasks = async () => {
      try {
        const response = await axios.get(`http://localhost:8000/assignedtasks/${userId}`);
        setTasks(response.data);
      } catch (error) {
        console.error('Error fetching user tasks:', error);
        setError('Failed to fetch user tasks');
      }
    };

    fetchUserTasks();
  }, [userId]);

  return (
    <div className="container">
      <h2>Tasks Assigned to You</h2>
      <div className="task-list">
        {tasks.length > 0 ? (
          tasks.map(task => (
            <div key={task.task_id} className="task">
              <h3>{task.title}</h3>
              <p>{task.description}</p>
              <p>Status: {task.status}</p>
              <p>Priority: {task.priority}</p>
            </div>
          ))
        ) : (
          <p>No tasks assigned to you</p>
        )}
      </div>
      {error && <p className="error">{error}</p>}
    </div>
  );
}

export default AssignedTasks;
