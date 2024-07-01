# Welcome to SuburbSavvy

SuburbSavvy is a simple first project exploring how to use PySpark, PostgreSQL and mySQL to load a sample dataset into PostgreSQL, followed by basic data cleaning and preparation tasks and then write to a mySQL database.

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/SuburbSavvy.git
   cd SuburbSavvy
   ```

2. **Set up the virtual environment using pipenv:**

   ```bash
   pip install pipenv
   pipenv install
   pipenv shell
   ```

3. **Set up PostgreSQL:**
   - Make sure you have PostgreSQL installed and running.
   - Create a database for the project.
   - Update the database connection settings in `config.py`.

## Usage

1. **Load the sample data into PostgreSQL and perform data cleaning and preparation:**
   ```bash
   python src/app.py
   ```
