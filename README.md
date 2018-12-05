# AAI_Impyla
Use AAI to query Impala via ODBC. Can easily be extended to query other ODBC sources.

Default engine name is "PyODBC" and runs on port 50052, set it accordingly in settings.ini (desktop) / analytic connections (server)

If running on windows, use anaconda for python, not base python, had better results with thriftpy library. It probably works fine on windows with the right deps installed.

Use python3.x, install packages "grpcio", "grpcio-tools" & "pyodbc"

~~~~~~~~~~~~~~~~~~~~~~~~~~

To Run:
1. Clone this git repo
2. Enter git repo directory
3. Run "python SQL_Plugin.py".. if successful you will see output *** Running server in insecure mode on port: 50052 ***
4. Start/Restart Qlik Sense Desktop or restart Qlik Sense Engine Service (if server) (the engine needs to be started with plugin running in background
(if correctly registered will see "Adding to capabilities: GetSQL(['sql'])")

NOTE: 
This is an early prototype to simply prove the concept simply, need to make the WHERE clause more dynamic by capturing the fields and related values selected.
