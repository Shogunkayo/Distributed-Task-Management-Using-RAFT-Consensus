// Navbar.js

import React from 'react';
import { Link, useParams } from 'react-router-dom';
import './Navbar.css'; // Import CSS file for styling

function Navbar() {
  const userId = sessionStorage.getItem('userId'); // Retrieve userId from URL parameters

  return (
    <div className="navbar-container">
      <Link to={`/addtask?userId=${userId}`} className= "navbar-link">Add Task</Link>
      <Link to={`/updatetask?userId=${userId}`} className="navbar-link">Update Task</Link>
      <Link to={`/deletetask?userId=${userId}`} className="navbar-link">Delete Task</Link>
      <Link to={`/alltasks/${userId}`} className="navbar-link">View All Tasks</Link>
      <Link to={`/assignedtasks?userId=${userId}`} className="navbar-link">View Assigned Tasks</Link>
      <Link to={`/assigntasks?userId=${userId}`} className="navbar-link">Assign Tasks</Link>
    </div>
  );
}

export default Navbar;
