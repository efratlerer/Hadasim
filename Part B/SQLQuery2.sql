CREATE TABLE Person (
    Person_Id INT PRIMARY KEY,
    Personal_Name VARCHAR(50),
    Family_Name VARCHAR(50),
    Gender VARCHAR(10),
    Father_Id INT,
    Mother_Id INT,
    Spouse_Id INT,
    
    -- Define foreign keys with names
    CONSTRAINT fk_father FOREIGN KEY (Father_Id) REFERENCES Person(Person_Id) 
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT fk_mother FOREIGN KEY (Mother_Id) REFERENCES Person(Person_Id) 
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT fk_spouse FOREIGN KEY (Spouse_Id) REFERENCES Person(Person_Id) 
        ON DELETE NO ACTION ON UPDATE NO ACTION,

    -- Prevent self-insertion
    CONSTRAINT chk_no_self_parent CHECK (Person_Id <> Father_Id AND Person_Id <> Mother_Id),
    CONSTRAINT chk_no_self_spouse CHECK (Person_Id <> Spouse_Id)
);

CREATE TABLE FamilyRelations (
    Person_Id INT NOT NULL,
    Relative_Id INT NOT NULL,
    Connection_Type VARCHAR(10) NOT NULL,

    CONSTRAINT fk_familyrelations_person FOREIGN KEY (Person_Id) REFERENCES Person(Person_Id),
    CONSTRAINT fk_familyrelations_relative FOREIGN KEY (Relative_Id) REFERENCES Person(Person_Id)
);

------------section A-----------

-- Insert into the FamilyRelations table
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
-- Father relationships
SELECT
    p.Person_Id,
    p.Father_Id AS Relative_Id,
    'Father' AS Connection_Type
FROM Person p
WHERE p.Father_Id IS NOT NULL

UNION ALL

 --Mother relationships
SELECT
    p.Person_Id,
    p.Mother_Id AS Relative_Id,
    'Mother' AS Connection_Type
FROM Person p
WHERE p.Mother_Id IS NOT NULL

UNION ALL

-- Spouse relationships
SELECT
    p1.Person_Id,
    p1.Spouse_Id AS Relative_Id,
    CASE p1.Gender
        WHEN 'Male' THEN 'Husband'
        WHEN 'Female' THEN 'Wife'
    END AS Connection_Type
FROM Person p1
WHERE p1.Spouse_Id IS NOT NULL

UNION ALL

 --Children relationships (through fathers)
SELECT
    f.Father_Id AS Person_Id,
    f.Person_Id AS Relative_Id,
    CASE f.Gender
        WHEN 'Male' THEN 'Son'
        WHEN 'Female' THEN 'Daughter'
    END AS Connection_Type
FROM Person f
WHERE f.Father_Id IS NOT NULL

UNION ALL

 --Children relationships (through mothers)
SELECT
    m.Mother_Id AS Person_Id,
    m.Person_Id AS Relative_Id,
    CASE m.Gender
        WHEN 'Male' THEN 'Son'
        WHEN 'Female' THEN 'Daughter'
    END AS Connection_Type
FROM Person m
WHERE m.Mother_Id IS NOT NULL

UNION ALL

-- Sibling relationships
SELECT
    p1.Person_Id,
    p2.Person_Id AS Relative_Id,
    CASE p2.Gender
        WHEN 'Male' THEN 'Brother'
        WHEN 'Female' THEN 'Sister'
    END AS Connection_Type
FROM Person p1
JOIN Person p2
    ON (p1.Father_Id = p2.Father_Id AND p1.Father_Id IS NOT NULL AND p1.Person_Id <> p2.Person_Id)
    OR (p1.Mother_Id = p2.Mother_Id AND p1.Mother_Id IS NOT NULL AND p1.Person_Id <> p2.Person_Id);

-- Display the new table
SELECT * FROM FamilyRelations
ORDER BY Person_Id, Connection_Type;


------------section B-----------

 --Complete missing spouse relationships
INSERT INTO FamilyRelations (Person_Id, Relative_Id, Connection_Type)
SELECT
    p1.Spouse_Id AS Person_Id,
    p1.Person_Id AS Relative_Id,
    CASE p1.Gender
        WHEN 'Male' THEN 'Wife' 
        WHEN 'Female' THEN 'Husband'
    END AS Connection_Type
FROM Person p1
WHERE p1.Spouse_Id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM FamilyRelations fr
      WHERE fr.Person_Id = p1.Spouse_Id
        AND fr.Relative_Id = p1.Person_Id
        AND fr.Connection_Type IN ('Husband', 'Wife')
  );

-- Display the updated table
SELECT * FROM FamilyRelations
ORDER BY Person_Id, Connection_Type;
