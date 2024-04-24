import React, { useState } from 'react';
import axios from 'axios';

function DeleteTask() {
  const [taskID, setTaskID] = useState('');
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');

  const deleteTask = async () => {
    if (!taskID) {
      setError('TaskID is required');
      return;
    }

    try {
      // Make API call to deleteTask endpoint
      const response = await axios.post('http://localhost:8000/deleteTask', {
        taskid: taskID
      });

      setMessage('Task deleted successfully');
    } catch (error) {
      console.error('Error deleting task:', error);
      setError('Failed to delete task');
    }
  };

  return (
    <div className="container">
      <h1>Delete Task</h1>
      <input type="text" placeholder="TaskID" value={taskID} onChange={e => setTaskID(e.target.value)} />
      <button onClick={deleteTask}>Delete Task</button>
      {error && <p className="error">{error}</p>}
      {message && <p className="success">{message}</p>}
    </div>
  );
}

export default DeleteTask;
