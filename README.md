Overview:

This project is a technical task that transforms government petition data from a JSON format into a CSV file with specific requirements. The input data consists of a set of government petitions, each containing a title (label), main text (abstract), and the number of signatures.

The goal is to extract meaningful information from this data and output it in a structured CSV format, containing:

	1.	A unique petition ID.
	2.	The 20 most common words (with 5 or more letters) across all petitions, and the count of each word for every petition.

Task Requirements:

	•	Programming Language: Python or PySpark.
	•	Testing: At least one test should be included.
	•	Best Practices: Keep coding best practices in mind (e.g., modularization, readability, and performance).
	•	Output: A CSV file with one row per petition and 21 columns (1 unique ID and 20 word frequency columns).

Input Data:

The input is a JSON file consisting of petitions with the following fields:

	•	label: The title of the petition.
	•	abstract: The main body of the petition.
	•	signatures: The number of signatures on the petition.

Output Data:

The output is a CSV file containing the following columns:

	•	petition_id: A unique identifier for each petition (not present in the input, needs to be generated).
	•	20 most common words: A column for each of the 20 most common words across all petitions, counting only words with 5 or more letters. The value in each column represents the count of that word in the respective petition.

For example, if “government” is one of the most common words:

	•	The CSV will have a government column.
	•	If Petition 1 includes “government” 3 times, and Petition 2 does not mention it, the values for those rows would be 3 and 0, respectively.

Steps:

	1.	Extract Data: Load the input data from the JSON file.
	2.	Transform Data:
	•	Tokenize the text of the petitions.
	•	Filter out words with fewer than 5 letters.
	•	Identify the 20 most common words across all petitions.
	3.	Create Unique Petition ID: Generate a unique identifier for each petition.
	4.	Count Word Frequencies: For each petition, count the occurrences of the 20 most common words.
	5.	Output to CSV: Export the transformed data to a CSV file with the required columns.


Installation and Setup:

	1.	Clone the Repository:
    git clone 
    cd petition-data-transformation

    2.	Install Dependencies:
    Ensure you have Python installed. Then, install required libraries:
    pip install -r requirements.txt

    3.	Run the Code:
    Execute the script to transform the data:
    python transform_petitions.py

    4.	Run the Tests:
    To run the test(s) included:
    pytest tests/

Project Structure:

    │   ├── petition_program_with_func.py  # Main transformation logic
    ├── data/
    │   ├── input_data.json                # Input JSON file (example data)
    ├── tests/
    │   ├── test_petition_program_with_func.py  # Test cases
    ├── README.md                          # Project documentation
    ├── requirements.txt                   # Python dependencies
    └── transform_petitions.py             # Script to execute the transformation


Testing:

I have included at least one test to verify the transformation logic. The tests check whether:

	•	The petition IDs are generated correctly.
	•	The word count for the 20 most common words is accurate.


Best Practices:

This project follows best practices, including:

	•	Modular code organization.
	•	Readable and maintainable code.
	•	Efficient data handling.
