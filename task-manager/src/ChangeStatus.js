// ChangeTaskStatus.js
import React, { useState } from 'react';
import axios from 'axios';
import './changestatus.css';

function ChangeStatus() {
  const [taskNo, setTaskNo] = useState('');
  const [status, setStatus] = useState('');
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [isOpen, setIsOpen] = useState(false);

  const handleChange = async () => {
    if (!taskNo || !status) {
      setError('Task number and status are required');
      return;
    }

    try {
      // Make API call to changeTaskStatus endpoint
      const response = await axios.post('http://localhost:8000/changeTaskStatus', {
        task_no: taskNo,
        status: status
      });

      setMessage('Task status updated successfully');
    } catch (error) {
      console.error('Error updating task status:', error);
      setError('Failed to update task status');
    }
  };

  const handleSelectChange = (e) => {
    setStatus(e.target.value);
    setIsOpen(false);
  };

  return (
    <div className="container">
      <h2>Change Task Status</h2>
      <div className="form-container">
        <input type="text" placeholder="Task Number" value={taskNo} onChange={e => setTaskNo(e.target.value)} />
        <div className="select-container">
          <select className="select" value={status} onChange={handleSelectChange} onClick={() => setIsOpen(true)}>
            <option value="" disabled hidden>Select Status</option>
            <option value="TODO">TODO</option>
            <option value="IN_PROGRESS">IN_PROGRESS</option>
            <option value="DONE">DONE</option>
          </select>
          <div className="select-arrow">&#9660;</div>
          <div className={`select-options ${isOpen ? 'open' : ''}`}>
            <div className="select-option" onClick={() => setStatus('TODO')}>TODO</div>
            <div className="select-option" onClick={() => setStatus('IN_PROGRESS')}>IN_PROGRESS</div>
            <div className="select-option" onClick={() => setStatus('DONE')}>DONE</div>
          </div>
        </div>
        <button onClick={handleChange}>Change Status</button>
      </div>
      {error && <p className="error">{error}</p>}
      {message && <p className="success">{message}</p>}
    </div>
  );
}

export default ChangeStatus;
