CREATE STREAM foodcoded_analyze WITH (kafka_topic = 'foodcoded_analyze') AS 
      SELECT
            foodcoded_clean.GPA,
            (CASE WHEN (GPA = 4)   THEN 'A'
                  WHEN (GPA >= 3.5 and GPA < 4)   THEN 'B+'
                  WHEN (GPA >= 3   and GPA < 3.5) THEN 'B'
                  WHEN (GPA >= 2.5 and GPA < 3)   THEN 'C+'
                  WHEN (GPA >= 2   and GPA < 2.5) THEN 'C'
                  WHEN (GPA >= 0   and GPA < 2)   THEN 'F'
             ELSE null END) Grade,
            foodcoded_clean.Gender,
            (CASE WHEN (foodcoded_clean.Gender = 1) THEN 'Female'
                  WHEN (foodcoded_clean.Gender = 2) THEN 'Male'
             ELSE null END) Gender_desc,
            foodcoded_clean.calories_day,
            (CASE WHEN (foodcoded_clean.calories_day = 1) THEN 'i dont know how many calories i should consume'
                  WHEN (foodcoded_clean.calories_day = 2) THEN 'it is not at all important'
                  WHEN (foodcoded_clean.calories_day = 3) THEN 'it is moderately important'
                  WHEN (foodcoded_clean.calories_day = 4) THEN 'it is very important'
             ELSE null END) calories_day_desc,
            foodcoded_clean.comfort_food_reasons_coded,
            (CASE WHEN (foodcoded_clean.comfort_food_reasons_coded = 1) THEN 'stress'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 2) THEN 'boredom'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 3) THEN 'depression/sadness'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 4) THEN 'hunger'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 5) THEN 'laziness'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 6) THEN 'cold weather'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 7) THEN 'happiness'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 8) THEN 'watching tv'
                  WHEN (foodcoded_clean.comfort_food_reasons_coded = 9) THEN 'none'
             ELSE null END) comfort_food_reasons_coded_desc,
            foodcoded_clean.cook,
            (CASE WHEN (foodcoded_clean.cook = 1) THEN 'Every day'
                  WHEN (foodcoded_clean.cook = 2) THEN 'A couple of times a week'
                  WHEN (foodcoded_clean.cook = 3) THEN 'Whenever I can, but that is not very often'
                  WHEN (foodcoded_clean.cook = 4) THEN 'I only help a little during holidays'
                  WHEN (foodcoded_clean.cook = 5) THEN 'Never, I really do not know my way around a kitchen'
             ELSE null END) cook_desc,
            foodcoded_clean.cuisine,
            (CASE WHEN (foodcoded_clean.cuisine = 1) THEN 'American'
                  WHEN (foodcoded_clean.cuisine = 2) THEN 'Mexican.Spanish'
                  WHEN (foodcoded_clean.cuisine = 3) THEN 'Korean/Asian'
                  WHEN (foodcoded_clean.cuisine = 4) THEN 'Indian'
                  WHEN (foodcoded_clean.cuisine = 5) THEN 'American inspired international dishes'
                  WHEN (foodcoded_clean.cuisine = 6) THEN 'other'
             ELSE null END) cuisine_desc,
            foodcoded_clean.diet_current_coded,
            (CASE WHEN (foodcoded_clean.diet_current_coded = 1) THEN 'healthy/balanced/moderated/'
                  WHEN (foodcoded_clean.diet_current_coded = 2) THEN 'unhealthy/cheap/too much/random/'
                  WHEN (foodcoded_clean.diet_current_coded = 3) THEN 'the same thing over and over'
                  WHEN (foodcoded_clean.diet_current_coded = 4) THEN 'unclear'
             ELSE null END) diet_current_coded_desc,
            foodcoded_clean.eating_out,
            (CASE WHEN (foodcoded_clean.eating_out = 1) THEN 'Never'
                  WHEN (foodcoded_clean.eating_out = 2) THEN '1-2 times'
                  WHEN (foodcoded_clean.eating_out = 3) THEN '2-3 times'
                  WHEN (foodcoded_clean.eating_out = 4) THEN '3-5 times'
                  WHEN (foodcoded_clean.eating_out = 5) THEN 'every day'
             ELSE null END) eating_out_desc,
            foodcoded_clean.employment,
            (CASE WHEN (foodcoded_clean.employment = 1) THEN 'yes full time'
                  WHEN (foodcoded_clean.employment = 2) THEN 'yes part time'
                  WHEN (foodcoded_clean.employment = 3) THEN 'no'
                  WHEN (foodcoded_clean.employment = 4) THEN 'other'
             ELSE null END) employment_desc,
            foodcoded_clean.ethnic_food,
            (CASE WHEN (foodcoded_clean.ethnic_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.ethnic_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.ethnic_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.ethnic_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.ethnic_food = 5) THEN 'very likely'
             ELSE null END) ethnic_food_desc,
            foodcoded_clean.exercise,
            (CASE WHEN (foodcoded_clean.exercise = 1) THEN 'Everyday'
                  WHEN (foodcoded_clean.exercise = 2) THEN 'Twice or three times per week'
                  WHEN (foodcoded_clean.exercise = 3) THEN 'Once a week'
                  WHEN (foodcoded_clean.exercise = 4) THEN 'Sometimes'
                  WHEN (foodcoded_clean.exercise = 4) THEN 'Never'
            ELSE null END) exercise_desc,
            foodcoded_clean.fav_cuisine,
            foodcoded_clean.fav_cuisine_coded,
            (CASE WHEN (foodcoded_clean.fav_cuisine_coded = 0) THEN 'none'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 1) THEN 'Italian/French/greek'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 2) THEN 'Spanish/mexican'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 3) THEN 'Arabic/Turkish'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 4) THEN 'asian/chineses/thai/nepal'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 5) THEN 'American'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 6) THEN 'African'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 7) THEN 'Jamaican'
                  WHEN (foodcoded_clean.fav_cuisine_coded = 8) THEN 'indian'
             ELSE null END) fav_cuisine_coded_desc,
            foodcoded_clean.fav_food,
            (CASE WHEN (foodcoded_clean.fav_food = 1) THEN 'cooked at home'
                  WHEN (foodcoded_clean.fav_food = 2) THEN 'store bought'
                  WHEN (foodcoded_clean.fav_food = 3) THEN 'both bought at store and cooked at home'
             ELSE null END) fav_food_desc,
            foodcoded_clean.food_childhood,
            (SUBSTRING(foodcoded_clean.food_childhood, 1, INSTR(foodcoded_clean.food_childhood, ','))) food_childhood_split,
            foodcoded_clean.fruit_day,
            (CASE WHEN (foodcoded_clean.fruit_day = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.fruit_day = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.fruit_day = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.fruit_day = 4) THEN 'likely'
                  WHEN (foodcoded_clean.fruit_day = 5) THEN 'very likely'
             ELSE null END) fruit_day_desc,
            foodcoded_clean.greek_food,
            (CASE WHEN (foodcoded_clean.greek_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.greek_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.greek_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.greek_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.greek_food = 5) THEN 'very likely'
             ELSE null END) greek_food_desc,
            foodcoded_clean.healthy_feeling,
            foodcoded_clean.income,
            (CASE WHEN (foodcoded_clean.income = 1) THEN 'less than $15,000'
                  WHEN (foodcoded_clean.income = 2) THEN '$15,001 to $30,000'
                  WHEN (foodcoded_clean.income = 3) THEN '$30,001 to $50,000'
                  WHEN (foodcoded_clean.income = 4) THEN '$50,001 to $70,000'
                  WHEN (foodcoded_clean.income = 5) THEN '$70,001 to $100,000'
                  WHEN (foodcoded_clean.income = 6) THEN 'higher than $100,000'
             ELSE null END) income_desc,
            foodcoded_clean.indian_food,
            (CASE WHEN (foodcoded_clean.indian_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.indian_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.indian_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.indian_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.indian_food = 5) THEN 'very likely'
            ELSE null END) indian_food_desc,
            foodcoded_clean.italian_food,
            (CASE WHEN (foodcoded_clean.italian_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.italian_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.italian_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.italian_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.italian_food = 5) THEN 'very likely'
             ELSE null END) italian_food_desc,
            foodcoded_clean.marital_status,
            (CASE WHEN (foodcoded_clean.marital_status = 1) THEN 'Single'
                  WHEN (foodcoded_clean.marital_status = 2) THEN 'In a relationship'
                  WHEN (foodcoded_clean.marital_status = 3) THEN 'Cohabiting'
                  WHEN (foodcoded_clean.marital_status = 4) THEN 'Married'
                  WHEN (foodcoded_clean.marital_status = 5) THEN 'Divorced'
                  WHEN (foodcoded_clean.marital_status = 6) THEN 'Widowed'
             ELSE null END) marital_status_desc,
            foodcoded_clean.nutritional_check,
            (CASE WHEN (foodcoded_clean.nutritional_check = 1) THEN 'never'
                  WHEN (foodcoded_clean.nutritional_check = 2) THEN 'on certain products only'
                  WHEN (foodcoded_clean.nutritional_check = 3) THEN 'very rarely'
                  WHEN (foodcoded_clean.nutritional_check = 4) THEN 'on most products'
                  WHEN (foodcoded_clean.nutritional_check = 5) THEN 'on everything'
            ELSE null END) nutritional_check_desc,
            foodcoded_clean.on_off_campus,
            (CASE WHEN (foodcoded_clean.on_off_campus = 1) THEN 'On campus'
                  WHEN (foodcoded_clean.on_off_campus = 2) THEN 'Rent out of campus'
                  WHEN (foodcoded_clean.on_off_campus = 3) THEN 'Live with my parents and commute'
                  WHEN (foodcoded_clean.on_off_campus = 4) THEN 'Own my own house'
             ELSE null END) on_off_campus_desc,
            foodcoded_clean.parents_cook,
            (CASE WHEN (foodcoded_clean.parents_cook = 1) THEN 'Almost everyday'
                  WHEN (foodcoded_clean.parents_cook = 2) THEN '2-3 times a week'
                  WHEN (foodcoded_clean.parents_cook = 3) THEN '1-2 times a week'
                  WHEN (foodcoded_clean.parents_cook = 4) THEN 'on holidays only'
                  WHEN (foodcoded_clean.parents_cook = 5) THEN 'never'
             ELSE null END) parents_cook_desc,
            foodcoded_clean.pay_meal_out,
            (CASE WHEN (foodcoded_clean.pay_meal_out = 1) THEN 'up to $5.00'
                  WHEN (foodcoded_clean.pay_meal_out = 2) THEN '$5.01 to $10.00'
                  WHEN (foodcoded_clean.pay_meal_out = 3) THEN '$10.01 to $20.00'
                  WHEN (foodcoded_clean.pay_meal_out = 4) THEN '$20.01 to $30.00'
                  WHEN (foodcoded_clean.pay_meal_out = 5) THEN '$30.01 to $40.00'
                  WHEN (foodcoded_clean.pay_meal_out = 6) THEN 'more than $40.01'
             ELSE null END) pay_meal_out_desc,
            foodcoded_clean.persian_food,
            (CASE WHEN (foodcoded_clean.persian_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.persian_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.persian_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.persian_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.persian_food = 5) THEN 'very likely'
             ELSE null END) persian_food_desc,
            foodcoded_clean.self_perception_weight,
            (CASE WHEN (foodcoded_clean.self_perception_weight = 1) THEN 'slim'
                  WHEN (foodcoded_clean.self_perception_weight = 2) THEN 'very fit'
                  WHEN (foodcoded_clean.self_perception_weight = 3) THEN 'just right'
                  WHEN (foodcoded_clean.self_perception_weight = 4) THEN 'slightly overweight'
                  WHEN (foodcoded_clean.self_perception_weight = 5) THEN 'overweight'
                  WHEN (foodcoded_clean.self_perception_weight = 6) THEN 'i dont think myself in these terms'
             ELSE null END) self_perception_weight_desc,
            foodcoded_clean.sports,
            (CASE WHEN (foodcoded_clean.sports = 1) THEN 'Yes'
                  WHEN (foodcoded_clean.sports = 2) THEN 'No'
             ELSE null END) sports_desc,
            foodcoded_clean.thai_food,
            (CASE WHEN (foodcoded_clean.thai_food = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.thai_food = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.thai_food = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.thai_food = 4) THEN 'likely'
                  WHEN (foodcoded_clean.thai_food = 5) THEN 'very likely'
             ELSE null END) thai_food_desc,
            foodcoded_clean.veggies_day,
            (CASE WHEN (foodcoded_clean.veggies_day = 1) THEN 'very unlikely'
                  WHEN (foodcoded_clean.veggies_day = 2) THEN 'unlikely'
                  WHEN (foodcoded_clean.veggies_day = 3) THEN 'neutral'
                  WHEN (foodcoded_clean.veggies_day = 4) THEN 'likely'
                  WHEN (foodcoded_clean.veggies_day = 5) THEN 'very likely'
             ELSE null END) veggies_day_desc,
            foodcoded_clean.vitamins,
            (CASE WHEN (foodcoded_clean.vitamins = 1) THEN 'Yes'
                  WHEN (foodcoded_clean.vitamins = 2) THEN 'No'
             ELSE null END) vitamins_desc,
            foodcoded_clean.weight

      FROM foodcoded_clean
      EMIT CHANGES;
