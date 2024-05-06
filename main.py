from database_helper import create_and_populate_database

def main():
    # Call the imported function
    create_and_populate_database()

# Add a conditional check to call the main function if this script is run as the main module
if __name__ == "__main__":
    main()