import psycopg2 as p

## Config later ##
dbServerName    = "localhost"
dbUser          = "postgres-user"
dbPassword      = "postgres-pw"
dbName          = "customers"

conn   = p.connect(host=dbServerName, user=dbUser, password=dbPassword,
                                     dbname=dbName)

cursor = conn.cursor()
cursor.execute('''
                CREATE TABLE IF NOT EXISTS foodcoded (
                    GPA varchar(10),
                    Gender int,
                    breakfast int,
                    calories_chicken int,
                    calories_day int,
                    calories_scone int,
                    coffee int,
                    comfort_food varchar(255),
                    comfort_food_reasons varchar(500),
                    comfort_food_reasons_coded int,
                    cook int,
                    comfort_food_reasons_coded1 int,
                    cuisine int,
                    diet_current varchar(500),
                    diet_current_coded int,
                    drink int,
                    eating_changes varchar(500),
                    eating_changes_coded int,
                    eating_changes_coded1 int,
                    eating_out int,
                    employment int,
                    ethnic_food int,
                    exercise int,
                    father_education int,
                    father_profession varchar(255),
                    fav_cuisine varchar(255),
                    fav_cuisine_coded int,
                    fav_food int,
                    food_childhood varchar(255),
                    fries int,
                    fruit_day int,
                    grade_level int,
                    greek_food int,
                    healthy_feeling int,
                    healthy_meal varchar(500),
                    ideal_diet varchar(500),
                    ideal_diet_coded int,
                    income int,
                    indian_food int,
                    italian_food int,
                    life_rewarding int,
                    marital_status int,
                    meals_dinner_friend varchar(500),
                    mother_education int,
                    mother_profession varchar(255),
                    nutritional_check int,
                    on_off_campus int,
                    parents_cook int,
                    pay_meal_out int,
                    persian_food int,
                    self_perception_weight int,
                    soup int,
                    sports int,
                    thai_food int,
                    tortilla_calories int,
                    turkey_calories int,
                    type_sports varchar(255),
                    veggies_day int,
                    vitamins int,
                    waffle_calories int,
                    weight varchar(255)
                )''')
conn.commit()
cursor.close()
conn.close()

print('create table')