import React, { useState } from 'react';
import axios from 'axios';

function UpdateTask({ userId }) {
  const [taskID, setTaskID] = useState('');
  const [title, setTitle] = useState('');
  const [description, setDescription] = useState('');
  const [status, setStatus] = useState('');
  const [priority, setPriority] = useState('');
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');

  const updateTask = async () => {
    if (!taskID || !title || !description || !status || !priority) {
      setError('All fields are required');
      return;
    }

    try {
      // Make API call to updateTask endpoint
      const response = await axios.post('http://localhost:8000/updateTask', {
        taskid: taskID,
        title,
        description,
        status,
        priority
      });

      setMessage('Task updated successfully');
    } catch (error) {
      console.error('Error updating task:', error);
      setError('Failed to update task');
    }
  };

  return (
    <div className="container">
      <h1>Update Task</h1>
      <input type="text" placeholder="TaskID" value={taskID} onChange={e => setTaskID(e.target.value)} />
      <input type="text" placeholder="Title" value={title} onChange={e => setTitle(e.target.value)} />
      <input type="text" placeholder="Description" value={description} onChange={e => setDescription(e.target.value)} />
      <input type="text" placeholder="Status" value={status} onChange={e => setStatus(e.target.value)} />
      <input type="text" placeholder="Priority" value={priority} onChange={e => setPriority(e.target.value)} />
      <button onClick={updateTask}>Update Task</button>
      {error && <p className="error">{error}</p>}
      {message && <p className="success">{message}</p>}
    </div>
  );
}

export default UpdateTask;
