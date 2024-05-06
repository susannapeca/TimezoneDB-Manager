# Time Zone Database Manager

The script uses SQLite to manage a database using information about time zones provided by the TimezoneDB API.
To get access to the API provided by TimezoneDB go to: https://timezonedb.com and create an account (free), you will be provided with a Key that you can use in your scripts.
## Steps for Running the Program

1. **Install requirements**: 
Install requirements using the following command:
```bash 
pip install -r requirements.txt
```

2.  **Replace API KEY**:
Go into the file api_key.py and replace the value of the API key with the value of your API key obtained on the TimezoneDB
website.


3. **Run the script**: Run the script using the following command:
```bash
python main.py
```

4. **Test the script**: Run the test.py file using the following command:
```bash
python test.py
```
You will be prompted to enter a table name, then the database table's contents will be displayed on the console.
