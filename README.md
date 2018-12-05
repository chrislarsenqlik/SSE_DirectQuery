# SSE_DirectQuery

Use AAI to query Impala via ODBC. Can easily be extended to query other ODBC sources.

Default engine name is "PyODBC" and runs on port 50052, set it accordingly in settings.ini (desktop) / analytic connections (server)

Use python3.x, install packages "grpcio", "grpcio-tools" & "pyodbc"
```
To Run:
1. Clone this git repo
2. Enter git repo directory
3. Run "python SQL_Plugin.py".. if successful you will see output *** Running server in insecure mode on port: 50052 ***
4. Start/Restart Qlik Sense Desktop or restart Qlik Sense Engine Service (if server) (the engine needs to be started with plugin running in background
(if correctly registered will see "Adding to capabilities: GetSQL(['sql'])")
```
This is an early prototype to simply prove the concept, improvements coming:
1. Visual query variable maker extension
2. Will make the WHERE clause logic variable more sophisticated/dynamic, then add as master library item
3. Currently there is an issue with python dying when too many clicks happen in a very short amount of time. Ideally wait for the query to finish. This might be an odbc driver limitation, need to test
