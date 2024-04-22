import React, { useState, useEffect } from 'react';
import axios from 'axios';
import {useNavigate} from 'react-router-dom';
import { Routes, Route} from 'react-router-dom'; // Import BrowserRouter, Routes, and Route components
import './App.css'; // Import CSS file for styling
import AddTask from './AddTask.js'; // Import AddTask component
import { set } from 'mongoose';


function App() {
  const [showLogin, setShowLogin] = useState(false);
  const [showRegister, setShowRegister] = useState(false);
  const [username, setUsername] = useState('');
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
      navigate(`/addtask?userId=${loggedInUser}`);
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
        }, 2000);

        
        navigate(`/addtask?userId=${response.data.userId}`);
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
        username,
        email,
        password
      });
      sessionStorage.setItem('userId', response.data.userId);
      setMessage('User registered successfully');
      setTimeout(() => {
        setShowLogin(false); // Hide login form after 2 seconds
        setShowRegister(false); // Hide register form after 2 seconds
      }, 2000);
      navigate(`/addtask?userId=${response.data.userId}`);
    } catch (error) {
      console.error('Error registering user:', error);
      setError('Failed to register user');
    }
  };


  return (
    <div className="container">
      <h1>Welcome to Task Manager</h1>
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
        <Route path="/addtask" element={<AddTask userId={userId}/>} />
      </Routes>
    </div>
  );
}

export default App;
