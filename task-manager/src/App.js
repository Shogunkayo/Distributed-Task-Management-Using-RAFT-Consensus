import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {useNavigate} from 'react-router-dom';
import { Routes, Route} from 'react-router-dom'; // Import BrowserRouter, Routes, and Route components
import './App.css'; // Import CSS file for styling
import AddTask from './AddTask.js'; // Import AddTask component
import { set } from 'mongoose';
import Navbar from './navbar.js'; // Sidebar component
import AllTasks from './AllTasks.js';
import UpdateTask from './UpdateTask.js';
import DeleteTask from './DeleteTask.js';
import AssignedTasks from './AssignedTasks.js';
import AssignTask from './AssignTask.js';
import Dashboard from './Dashboard.js';


function App() {
  const [showLogin, setShowLogin] = useState(false);
  const [showRegister, setShowRegister] = useState(false);
  const [username, setUsername] = useState('');
  const [userid, setUserID] = useState('')
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [userId, setUserId] = useState(''); 
  const navigate = useNavigate(); // Add useNavigation hook for navigation

  useEffect (() => {
    const loggedInUser = sessionStorage.getItem("userId");
    if (loggedInUser) {
      setUserId(loggedInUser);
      navigate(`/dashboard`);
    }
  }, [navigate]);

  const loginUser = async () => {
    if (!username || !password) {
      setError('Username and password are required');
      return;
    }

    try {
      // Make API call to login endpoint
      const response = await axios.post('http://localhost:8000/checkUser', {
        username,
        password
      });

      console.log('Login response:', response.data)

      if (response.data.exists) {
        sessionStorage.setItem('userId', response.data.userId);
        setMessage('Login Successful');
        setShowLogin(false); // Hide login form after 2 seconds
        setShowRegister(false); // Clear the message after 2 seconds
        setError('')
        setTimeout(() => {
          setMessage('');
          navigate(`dashboard`);
        }, 2000);

      } else {
        setError('Invalid username or password');
      }
    } catch (error) {
      console.error('Error logging in:', error);
      setError('Failed to login');
    }
  };

  const registerUser = async () => {
    if (!username || !email || !password) {
      setError('Username, email, and password are required');
      return;
    }

    try {
      const response = await axios.post('http://localhost:8000/addUser', {
        userid,
        username,
        email,
        password
      });
      sessionStorage.setItem('userId', userid);
      setMessage('User registered successfully');
      setShowLogin(false); // Hide login form after 2 seconds
      setShowRegister(false); // Hide register form after 2 seconds
      setTimeout(() => {
      navigate(`/dashboard`);  
      }, 2000);
      
    } catch (error) {
      console.error('Error registering user:', error);
      setError('Failed to register user');
    }
  };


  return (
    <div className="container">
      <h1>Task Manager</h1>
      {userId && <Navbar />}

      {!userId && !showLogin && !showRegister && (
        <>
          <button onClick={() => setShowLogin(true)}>Login</button>
          <button onClick={() => setShowRegister(true)}>Register</button>
        </>
      )}
      {showLogin && (
        <div className="form-container">
          <h2>Login</h2>
          <input type="text" placeholder="Username" value={username} onChange={e => setUsername(e.target.value)} />
          <input type="password" placeholder="Password" value={password} onChange={e => setPassword(e.target.value)} />
          <button onClick={loginUser}>Login</button>
          <button onClick={() => setShowLogin(false)}>Cancel</button>
        </div>
      )}
      {showRegister && (
        <div className="form-container">
          <h2>Register</h2>
          <input type="text" placeholder="User ID" value={userid} onChange={e => setUserID(e.target.value)} />
          <input type="text" placeholder="Username" value={username} onChange={e => setUsername(e.target.value)} />
          <input type="text" placeholder="Email" value={email} onChange={e => setEmail(e.target.value)} />
          <input type="password" placeholder="Password" value={password} onChange={e => setPassword(e.target.value)} />
          <button onClick={registerUser}>Register</button>
          <button onClick={() => setShowRegister(false)}>Cancel</button>
        </div>
      )}
      {error && <p className="error">{error}</p>}
      {message && <p className="success">{message}</p>}

      <Routes>
        <Route path ="/dashboard" element={<Dashboard/>}/>
        <Route path="/addtask" element={userId && <AddTask userId={userId}/>} />
        <Route path="/alltasks/:userId" element={<AllTasks/>} />
        <Route path="/updatetask" element={<UpdateTask />} />
      </Routes>
    </div>
  );
}

export default App;
