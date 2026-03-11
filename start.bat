@echo off
echo ============================================
echo  BTV Airspace 3D Viewer
echo  Burlington VT - 50nm radius
echo ============================================
echo.
echo Starting local web server...
echo Open your browser to: http://localhost:8080
echo Press Ctrl+C to stop the server.
echo.
start http://localhost:8080
"%LOCALAPPDATA%\Programs\Python\Python312\python.exe" -m http.server 8080
