import sqlalchemy

def main():
  print("Generate random SQLAlchemy queries!")
  postgres_engine = sqlalchemy.create_engine("postgresql://postgres:postgres@localhost:5438/dvdrental")
  with postgres_engine.connect() as connection:
      result = connection.execute(sqlalchemy.text("select * from actor"))
      print(result)
      for row in result:
          print("actor:", row)
  
if __name__ == "__main__":
  main()