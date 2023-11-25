from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.debug = True

# adding configuration for using a sqlite database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'

# Creating an SQLAlchemy templates
db = SQLAlchemy(app)

class EmployeeModel(db.Model):
    __tablename__="employee"
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(20), unique=False, nullable=False)
    last_name = db.Column(db.String(20), unique=False, nullable=False)
    age = db.Column(db.Integer, nullable=False)

    # repr method represents how one object of this datatable
    # will look like
    def __init__(self, first_name,last_name, age):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age

        def __repr__(self):
                return f"{self.first_name}: {self.age}"


if __name__ == '__main__':
    app.run()
