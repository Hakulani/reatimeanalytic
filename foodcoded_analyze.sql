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
            foodcoded_clean.food_childhood,
            foodcoded_clean.fruit_day,
            foodcoded_clean.greek_food,
            foodcoded_clean.healthy_feeling,
            foodcoded_clean.income,
            foodcoded_clean.indian_food,
            foodcoded_clean.italian_food,
            foodcoded_clean.marital_status,
            foodcoded_clean.nutritional_check,
            foodcoded_clean.on_off_campus,
            foodcoded_clean.parents_cook,
            foodcoded_clean.pay_meal_out,
            foodcoded_clean.persian_food,
            foodcoded_clean.self_perception_weight,
            foodcoded_clean.sports,
            foodcoded_clean.thai_food,
            foodcoded_clean.veggies_day,
            foodcoded_clean.vitamins,
            foodcoded_clean.weight

      FROM foodcoded_clean
      EMIT CHANGES;
