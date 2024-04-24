import React, { useState } from 'react';
import axios from 'axios';

function AssignTask() {
  const [assignmentID, setAssignmentID] = useState('');
  const [taskID, setTaskID] = useState('');
  const [userID, setUserID] = useState('');
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');

  const assignTask = async () => {
    if (!assignmentID || !taskID || !userID) {
      setError('All fields are required');
      return;
    }

    try {
      // Make API call to assignTask endpoint
      const response = await axios.post('http://localhost:8000/assignTask', {
        assignmentid: assignmentID,
        taskid: taskID,
        userid: userID
      });

      setMessage('Task assigned successfully');
    } catch (error) {
      console.error('Error assigning task:', error);
      setError('Failed to assign task');
    }
  };

  return (
    <div className="container">
      <h1>Assign Task</h1>
      <input type="text" placeholder="AssignmentID" value={assignmentID} onChange={e => setAssignmentID(e.target.value)} />
      <input type="text" placeholder="TaskID" value={taskID} onChange={e => setTaskID(e.target.value)} />
      <input type="text" placeholder="UserID" value={userID} onChange={e => setUserID(e.target.value)} />
      <button onClick={assignTask}>Assign Task</button>
      {error && <p className="error">{error}</p>}
      {message && <p className="success">{message}</p>}
    </div>
  );
}

export default AssignTask;
