// AddTask.js
import React, { useState } from 'react';
import axios from 'axios';

function AddTask({ userId }) {
  const [title, setTitle] = useState('');
  const [description, setDescription] = useState('');
  const [status, setStatus] = useState('');
  const [priority, setPriority] = useState('');
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [taskid, setTaskID] = useState('');

  const addTask = async () => {
    if (!taskid || !title || !description || !status || !priority) {
      setError('All fields are required');
      return;
    }

    try {
      // Make API call to addTask endpoint
      const response = await axios.post('http://localhost:8000/updatetask', {
        taskid,
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
      <input type="text" placeholder="TaskID" value={taskid} onChange={e => setTaskID(e.target.value)} />
      <input type="text" placeholder="Title" value={title} onChange={e => setTitle(e.target.value)} />
      <input type="text" placeholder="Description" value={description} onChange={e => setDescription(e.target.value)} />
      <input type="text" placeholder="Status" value={status} onChange={e => setStatus(e.target.value)} />
      <input type="text" placeholder="Priority" value={priority} onChange={e => setPriority(e.target.value)} />
      <button onClick={addTask}>Update Task</button>
      {error && <p className="error">{error}</p>}
      {message && <p className="success">{message}</p>}
    </div>
  );
}

export default AddTask;
