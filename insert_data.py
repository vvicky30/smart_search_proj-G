from config import SessionLocal
from sqlalchemy import text #we should use SQLAlchemy's text construct to ensure the raw SQL string is interpreted correctly. 
#probably for the potential ERROR like :
#The error message we can potentially received indicates that SQLAlchemy is having trouble interpreting the raw-SQL-string in the session.execute command.
# This can happen when SQLAlchemy needs a clear indication that you're passing a raw SQL string.

# Create a list of sample documents
sample_documents = [
    "i Have a dream that you will mine.",
    "Enter the sandman like a wolf in the snow.",
    "Why you are afraid of these clowns.",
    "i am going to horror house for fear less adventure.",
    "you are just another brick on the wall."
]

# Function to insert sample data into the database
def insert_sample_data():
    session = SessionLocal()  # making object by sessionLocal's constructor call
    #try and except the code here while excecuting queries to avoid the unwanted program termination due to postgre-sql side error. 
    try:
        for content in sample_documents:  # traversing over sample_documents through content-var (iterator)
            # now inserting each row from sample_document as content to the table - documents  
            # Use the text() construct to handle raw SQL
            session.execute(
                text("INSERT INTO documents (content) VALUES (:content)"), 
                {'content': content}
            )
        session.commit()
        print("Sample data inserted successfully.")
    except Exception as e: # if above try-block not excecuted ; then except the corresponding error ; rollback ; then print corresponding error
        session.rollback()  # rollback 
        print("Error inserting data:", e)
    finally:  # session have to close so that sqlAlchemy resources which's alloted while calling consturctor now have to released like that : 
        session.close()

# Run the function
if __name__ == "__main__":
    insert_sample_data()
