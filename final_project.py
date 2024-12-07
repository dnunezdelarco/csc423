import sqlite3
import pandas as pd

# Connects to an existing database file in the current directory
# If the file does not exist, it creates it in the current directory
db_connect = sqlite3.connect('test.db')

# Instantiate cursor object for executing queries
cursor = db_connect.cursor()

# Reset tables
tables_to_reset = ["Examination", "Pet", "Owner", "Animal", "Staff", "Clinic", "Position"]
for table in tables_to_reset:
    cursor.execute(f"DROP TABLE IF EXISTS {table};")

# Create Position table and execute query
query = """
    CREATE TABLE Position (
    positionNo VARCHAR(6) NOT NULL,
    positionName VARCHAR(50) NOT NULL,
    salary DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (positionNo),
    CONSTRAINT check_positive_salary CHECK (salary > 0));
    """
cursor.execute(query)

# Insert rows into Position table
query = """
    INSERT INTO Position (positionNo, positionName, salary) 
    VALUES 
        ('J00001', 'Senior Veterinarian', 120000.00),
        ('J00002', 'Veterinarian', 75000.00),
        ('J00003', 'Veterinary Technician', 65000.00),
        ('J00004', 'Veterinary Nurse', 55000.00),
        ('J00005', 'Receptionist', 40000.00);
    """
cursor.execute(query)

# Select Position data
query = """
    SELECT * 
    FROM Position; 
    """
cursor.execute(query)
print("Position table:\n")
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]),"\n")

# Create Clinic table and execute query
query = """
    CREATE TABLE Clinic (
    clinicNo VARCHAR(6) NOT NULL,
    clinicName VARCHAR(50) NOT NULL,
    clinicAddress VARCHAR(100) NOT NULL,
    clinicTelephone NUMBER(10) NOT NULL,
    manager VARCHAR(6),
    PRIMARY KEY (clinicNo));
    """
cursor.execute(query)

# Insert rows into Clinic table
query = """
    INSERT INTO Clinic (clinicNo, clinicName, clinicAddress, clinicTelephone, manager) 
    VALUES
        ('C00001', 'Downtown Pawsome Clinic', '123 Downtown St, Miami, FL', 3051234567,'S00001'),
        ('C00002', 'South Beach Pawsome Clinic', '456 South Beach Ave, Miami, FL', 3052345678,'S00007'),
        ('C00003', 'Little Havana Pawsome Clinic', '789 Little Havana Blvd, Miami, FL', 3053456789,'S00008'),
        ('C00004', 'Coral Gables Pawsome Clinic', '321 Coral Gables Rd, Miami, FL', 3054567890,'S00009'),
        ('C00005', 'Midtown Pawsome Clinic', '654 Midtown Dr, Miami, FL', 3055678901,'S00006');
    """
cursor.execute(query)

# Create Staff table and execute query
query = """
    CREATE TABLE Staff (
    staffNo VARCHAR(6) NOT NULL,
    staffFName VARCHAR(50) NOT NULL,
    staffLName VARCHAR(50) NOT NULL,
    staffAddress VARCHAR(100),
    staffDOB DATE,
    positionNo VARCHAR(6) NOT NULL,
    clinicNo VARCHAR(6) NOT NULL,
    PRIMARY KEY (staffNo),
    FOREIGN KEY (positionNo) REFERENCES Position(positionNo));
    """
cursor.execute(query)

# Insert rows into Staff table
query = """
    INSERT INTO Staff (staffNo, staffFName, staffLName, staffAddress, staffDOB, positionNo, clinicNo) 
    VALUES
        ('S00001', 'Jessie', 'Smith', '123 Elm Street, Miami, FL', '1985-06-15', 'J00001', 'C00001'),
        ('S00002', 'Amy', 'Johnson', '456 Oak Avenue, Miami, FL', '1990-03-22', 'J00002', 'C00002'),
        ('S00003', 'Elizabeth', 'McDonald', '789 Pine Road, Miami, FL', '1988-11-01', 'J00003', 'C00003'),
        ('S00004', 'Carl', 'Peterson', '321 Palm Blvd, Miami, FL', '1975-05-09', 'J00004', 'C00004'),
        ('S00005', 'Roger', 'Martin', '654 Maple Lane, Miami, FL', '1995-07-18', 'J00005', 'C00005'),
        ('S00006', 'David', 'Wilson', '101 Biscayne Blvd, Miami, FL', '1982-08-25', 'J00001', 'C00005'),
        ('S00007', 'Laura', 'Martinez', '202 Brickell Ave, Miami, FL', '1992-12-05', 'J00001', 'C00002'),
        ('S00008', 'Mark', 'Garcia', '303 Coconut Grove Rd, Miami, FL', '1986-04-14', 'J00001', 'C00003'),
        ('S00009', 'Sophia', 'Taylor', '404 Wynwood Blvd, Miami, FL', '1990-10-23', 'J00001', 'C00004'),
        ('S00010', 'Daniel', 'Anderson', '505 Design District St, Miami, FL', '1993-03-17', 'J00005', 'C00005');
    """
cursor.execute(query)

# Disable foreign key constraints
cursor.execute("PRAGMA foreign_keys = OFF;")

# Backup Clinic data
cursor.execute("CREATE TEMPORARY TABLE Clinic_backup AS SELECT * FROM Clinic;")

# Drop original Clinic table
cursor.execute("DROP TABLE Clinic;")

# Recreate Clinic table with FOREIGN KEY
cursor.execute("""
CREATE TABLE Clinic (
    clinicNo TEXT NOT NULL PRIMARY KEY,
    clinicName TEXT NOT NULL,
    clinicAddress TEXT NOT NULL,
    clinicTelephone TEXT NOT NULL,
    manager TEXT,
    UNIQUE (manager),
    FOREIGN KEY (manager) REFERENCES Staff(staffNo));
""")

# Restore Clinic data
cursor.execute("INSERT INTO Clinic SELECT * FROM Clinic_backup;")

# Drop the temporary table
cursor.execute("DROP TABLE Clinic_backup;")

# Select Clinic table
query = """
    Select * from Clinic;
    """
cursor.execute(query)
print("Clinic table:\n")
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]),"\n")

# Backup Staff data
cursor.execute("CREATE TEMPORARY TABLE Staff_backup AS SELECT * FROM Staff;")

# Drop original Staff table
cursor.execute("DROP TABLE Staff;")

# Recreate Staff table with FOREIGN KEY
cursor.execute("""
 CREATE TABLE Staff (
    staffNo VARCHAR(6) NOT NULL,
    staffFName VARCHAR(50) NOT NULL,
    staffLName VARCHAR(50) NOT NULL,
    staffAddress VARCHAR(100),
    staffDOB DATE,
    positionNo VARCHAR(6) NOT NULL,
    clinicNo VARCHAR(6) NOT NULL,
    PRIMARY KEY (staffNo),
    FOREIGN KEY (positionNo) REFERENCES Position(positionNo)
    FOREIGN KEY (clinicNo) REFERENCES Clinic(clinicNo));
""")

# Restore Staff data
cursor.execute("INSERT INTO Staff SELECT * FROM Staff_backup;")

# Drop the temporary table
cursor.execute("DROP TABLE Staff_backup;")

# Select Staff table
query = """
    Select * from Staff;
    """
cursor.execute(query)
print("Staff table:\n")
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]),"\n")

# Reenable foreign key constraints
cursor.execute("PRAGMA foreign_keys = ON;")

# Create Animal table and execute query
query = """
    CREATE TABLE Animal (
    species VARCHAR(50) NOT NULL,
    breed VARCHAR(50) NOT NULL,
    PRIMARY KEY (species, breed));
    """
cursor.execute(query)

# Insert rows into Animal table
query = """
    INSERT INTO Animal (species, breed)
    VALUES
        ('Dog', 'Schnauzer'),
        ('Dog', 'Dobermann'),
        ('Dog', 'Golden Retriever'),
        ('Cat', 'Persian'),
        ('Cat', 'Siamese'),
        ('Dog', 'Labrador');
    """
cursor.execute(query)

# Select Animal data
query = """
    SELECT * 
    FROM Animal; 
    """
cursor.execute(query)
print("Animal table:\n")
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]),"\n")

# Create Owner table and execute query
query = """
    CREATE TABLE Owner (
    ownerNo VARCHAR(6) NOT NULL,
    ownerFName VARCHAR(50) NOT NULL,
    ownerLName VARCHAR(50) NOT NULL,
    ownerAddress VARCHAR(100) NOT NULL,
    ownerTelephone NUMBER(10) NOT NULL,
    PRIMARY KEY (ownerNo));
    """
cursor.execute(query)

# Insert rows into Owner table
query = """
    INSERT INTO Owner (ownerNo, ownerFName, ownerLName, ownerAddress, ownerTelephone)
    VALUES 
        ('O00001', 'Alice', 'Smith', '123 Main St, Miami, FL', 3051234567),
        ('O00002', 'Bob', 'Johnson', '456 Oak Ave, Miami, FL', 3052345678),
        ('O00003', 'Catherine', 'Williams', '789 Pine Rd, Miami, FL', 3053456789),
        ('O00004', 'Daniel', 'Brown', '321 Elm St, Miami, FL', 3054567890),
        ('O00005', 'Emily', 'Davis', '654 Maple Lane, Miami, FL', 3055678901),
        ('O00006', 'Frank', 'Wilson', '987 Palm Blvd, Miami, FL', 3056789012);
    """
cursor.execute(query)

# Select Owner data
query = """
    SELECT * 
    FROM Owner; 
    """
cursor.execute(query)
print("Owner table:\n")
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]),'\n')

# Create Pet table and execute query
query = """
    CREATE TABLE Pet (
    petNo VARCHAR(6) NOT NULL,
    petDOB DATE NOT NULL,
    species VARCHAR(50) NOT NULL,
    breed VARCHAR(50) NOT NULL,
    color VARCHAR(50),
    clinicNo VARCHAR(6) NOT NULL,
    ownerNo VARCHAR(6) NOT NULL,
    PRIMARY KEY (petNo),
    FOREIGN KEY (ownerNo) REFERENCES Owner(ownerNo)
    	ON DELETE CASCADE,
    FOREIGN KEY (clinicNo) REFERENCES Clinic(clinicNo),
    FOREIGN KEY (species, breed) REFERENCES Animal(species, breed));
    """
cursor.execute(query)

# Insert rows into Pet table
query = """
    INSERT INTO Pet (petNo, petDOB, species, breed, color, clinicNo, ownerNo)
VALUES
    ('P00001', '2015-01-09', 'Dog', 'Schnauzer', 'Gray', 'C00004', 'O00006'),
    ('P00002', '2019-07-14', 'Dog', 'Dobermann', 'Brown', 'C00003', 'O00005'),
    ('P00003', '2021-10-19', 'Cat', 'Siamese', 'Beige', 'C00001', 'O00001'),
    ('P00004', '2024-03-15', 'Cat', 'Persian', 'White', 'C00002', 'O00002'),
    ('P00005', '2018-12-23', 'Dog', 'Golden Retriever', 'Yellow', 'C00005', 'O00004'),
    ('P00006', '2018-11-04', 'Cat', 'Persian', 'White', 'C00002', 'O00003');
    """
cursor.execute(query)

# Select Pet data
query = """
    SELECT * 
    FROM Pet; 
    """
cursor.execute(query)
print("Pet table:\n")
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]),"\n")

# Create Examination table and execute query
query = """
    CREATE TABLE Examination (
    examNo VARCHAR(6) NOT NULL,
    complaint VARCHAR(100) NOT NULL,
    description VARCHAR(100) NOT NULL,
    examDate DATE NOT NULL,
    examAction VARCHAR(100) NOT NULL,
    petNo VARCHAR(6) NOT NULL,
    staffNo VARCHAR(6) DEFAULT 'S00000' NOT NULL,
    PRIMARY KEY (examNo),
    FOREIGN KEY (petNo) REFERENCES Pet(petNo)
        ON DELETE CASCADE,
    FOREIGN KEY (staffNo) REFERENCES Staff(staffNo));
    """
cursor.execute(query)

# Insert rows into Examination table
query = """
    INSERT INTO Examination (examNo, complaint, description, examDate, examAction, petNo, staffNo)
VALUES
    ('E00001', 'Limping', 'The pet is limping on its front right leg.', '2024-03-14', 'Applied bandage and rest', 'P00003', 'S00001'),
    ('E00002', 'Vomiting', 'The pet has been vomiting frequently.', '2024-06-15', 'Administered anti-nausea medication', 'P00004', 'S00002'),
    ('E00003', 'Fever', 'The pet has a high fever.', '2024-07-02', 'Prescribed fever reducer', 'P00001', 'S00003'),
    ('E00004', 'Skin irritation', 'The pet has itching and rash on the back.', '2024-09-08', 'Applied topical cream', 'P00002', 'S00004'),
    ('E00005', 'Ear infection', 'The pet is scratching its ears frequently.', '2024-10-15', 'Prescribed ear drops', 'P00005', 'S00005'),
    ('E00006', 'Coughing', 'The pet has been coughing for a few days.', '2024-10-20', 'Prescribed cough syrup', 'P00006', 'S00006');
    """
cursor.execute(query)

# Select Examination data
query = """
    SELECT * 
    FROM Examination; 
    """
cursor.execute(query)
print("Examination table:\n")
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]),"\n")

#Perform queries
print("Queries\n")

# 1. Register a new pet
print("1. Register a new pet - add Milan Diaz’s golden retriever to the database:\n")
query = """
    SELECT * FROM Pet
    ORDER BY petNo;
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

query = """
    SELECT * FROM Owner
    ORDER BY ownerNo;
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

query = """
    INSERT INTO Owner (ownerNo, ownerFName, ownerLName, ownerAddress, ownerTelephone)
    VALUES ('O00007', 'Milan', 'Diaz', 'Coral Gables, Miami, FL', 3056789012);
    """
cursor.execute(query)

query = """
    INSERT INTO Pet (petNo, petDOB, species, breed, color, clinicNo, ownerNo)
    VALUES ('P00007', '2020-02-11', 'Dog', 'Golden Retriever', 'Yellow', 'C00001', 'O00007');
    """
cursor.execute(query)

query = """
    SELECT * FROM Pet
    ORDER BY petNo;
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

query = """
    SELECT * FROM Owner
    ORDER BY ownerNo;
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

# 2. Update a clinic’s manager
print("2. Update a clinic’s manager - make Elizabeth McDonald manager for the Little Havana branch:\n")
query = """
    SELECT * FROM Staff
    WHERE staffFName LIKE '%Elizabeth%'
    AND staffLName LIKE '%McDonald%'
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

query = """
    SELECT * FROM Clinic 
    WHERE clinicAddress LIKE '%Little Havana%';
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

query = """
    UPDATE Clinic
    SET manager = (
        SELECT staffNo FROM Staff
        WHERE staffFName LIKE '%Elizabeth%'
        AND staffLName LIKE '%McDonald%')
    WHERE clinicNo = (
        SELECT clinicNo FROM Clinic 
        WHERE clinicAddress LIKE '%Little Havana%')
    ;
    """
cursor.execute(query)

query = """
    SELECT * FROM Clinic 
    WHERE clinicAddress LIKE '%Little Havana%';
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

# 3. Find how many pets a specific owner has
print("3. Find how many pets Catherine Williams has:\n")
query = """
    SELECT COUNT(*) AS "Catherine Williams' No. of Pets"
    FROM Pet
    WHERE ownerNo = (SELECT ownerNo
        FROM Owner
        WHERE ownerFName LIKE '%Catherine%'
        AND ownerLName LIKE '%Williams%');
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

# 4. Find the average salary of each position
print("4. Find the average salary of each position:\n")
query = """
    SELECT positionName, AVG(salary) AS averageSalary
    FROM Position
    GROUP BY positionName;
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

# 5. Count the number of pets registered in each clinic
print("5. Count the number of pets registered in each clinic:\n")
query = """
    SELECT clinicNo, COUNT(*) AS petCounter
    FROM Pet
    GROUP BY clinicNo;
    """
cursor.execute(query)
print(pd.DataFrame(cursor.fetchall(), columns=[row[0] for row in cursor.description]))
print("")

# Commit any changes to the database
db_connect.commit()

# Close the connection if we are done with it.
db_connect.close()
