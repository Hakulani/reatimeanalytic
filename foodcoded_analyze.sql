CREATE STREAM food_analyze WITH (kafka_topic = 'food_analyze') AS 
      SELECT
            food_clean.GPA,
            (CASE WHEN (GPA = 4)   THEN 'A'
                  WHEN (GPA >= 3.5 and GPA < 4)   THEN 'B+'
                  WHEN (GPA >= 3   and GPA < 3.5) THEN 'B'
                  WHEN (GPA >= 2.5 and GPA < 3)   THEN 'C+'
                  WHEN (GPA >= 2   and GPA < 2.5) THEN 'C'
                  WHEN (GPA >= 0   and GPA < 2)   THEN 'F'
             ELSE null END) Grade,
            food_clean.Gender,
            (CASE WHEN (food_clean.Gender = 1) THEN 'Female'
                  WHEN (food_clean.Gender = 2) THEN 'Male'
             ELSE null END) Gender_desc,
            food_clean.calories_day,
            (CASE WHEN (food_clean.calories_day = 1) THEN 'i dont know how many calories i should consume'
                  WHEN (food_clean.calories_day = 2) THEN 'it is not at all important'
                  WHEN (food_clean.calories_day = 3) THEN 'it is moderately important'
                  WHEN (food_clean.calories_day = 4) THEN 'it is very important'
             ELSE null END) calories_day_desc,
            food_clean.comfort_food_reasons_coded,
            (CASE WHEN (food_clean.comfort_food_reasons_coded = 1) THEN 'stress'
                  WHEN (food_clean.comfort_food_reasons_coded = 2) THEN 'boredom'
                  WHEN (food_clean.comfort_food_reasons_coded = 3) THEN 'depression/sadness'
                  WHEN (food_clean.comfort_food_reasons_coded = 4) THEN 'hunger'
                  WHEN (food_clean.comfort_food_reasons_coded = 5) THEN 'laziness'
                  WHEN (food_clean.comfort_food_reasons_coded = 6) THEN 'cold weather'
                  WHEN (food_clean.comfort_food_reasons_coded = 7) THEN 'happiness'
                  WHEN (food_clean.comfort_food_reasons_coded = 8) THEN 'watching tv'
                  WHEN (food_clean.comfort_food_reasons_coded = 9) THEN 'none'
             ELSE null END) comfort_food_reasons_coded_desc,
            food_clean.cuisine,
            (CASE WHEN (food_clean.cuisine = 1) THEN 'American'
                  WHEN (food_clean.cuisine = 2) THEN 'Mexican.Spanish'
                  WHEN (food_clean.cuisine = 3) THEN 'Korean/Asian'
                  WHEN (food_clean.cuisine = 4) THEN 'Indian'
                  WHEN (food_clean.cuisine = 5) THEN 'American inspired international dishes'
                  WHEN (food_clean.cuisine = 6) THEN 'other'
             ELSE null END) cuisine_desc,
            food_clean.diet_current_coded,
            (CASE WHEN (food_clean.diet_current_coded = 1) THEN 'healthy/balanced/moderated/'
                  WHEN (food_clean.diet_current_coded = 2) THEN 'unhealthy/cheap/too much/random/'
                  WHEN (food_clean.diet_current_coded = 3) THEN 'the same thing over and over'
                  WHEN (food_clean.diet_current_coded = 4) THEN 'unclear'
             ELSE null END) diet_current_coded_desc,
            food_clean.eating_out,
            (CASE WHEN (food_clean.eating_out = 1) THEN 'Never'
                  WHEN (food_clean.eating_out = 2) THEN '1-2 times'
                  WHEN (food_clean.eating_out = 3) THEN '2-3 times'
                  WHEN (food_clean.eating_out = 4) THEN '3-5 times'
                  WHEN (food_clean.eating_out = 5) THEN 'every day'
             ELSE null END) eating_out_desc,
            food_clean.employment,
            (CASE WHEN (food_clean.employment = 1) THEN 'yes full time'
                  WHEN (food_clean.employment = 2) THEN 'yes part time'
                  WHEN (food_clean.employment = 3) THEN 'no'
                  WHEN (food_clean.employment = 4) THEN 'other'
             ELSE null END) employment_desc,
            food_clean.ethnic_food,
            (CASE WHEN (food_clean.ethnic_food = 1) THEN 'very unlikely'
                  WHEN (food_clean.ethnic_food = 2) THEN 'unlikely'
                  WHEN (food_clean.ethnic_food = 3) THEN 'neutral'
                  WHEN (food_clean.ethnic_food = 4) THEN 'likely'
                  WHEN (food_clean.ethnic_food = 5) THEN 'very likely'
             ELSE null END) ethnic_food_desc,
            food_clean.exercise,
            (CASE WHEN (food_clean.exercise = 1) THEN 'Everyday'
                  WHEN (food_clean.exercise = 2) THEN 'Twice or three times per week'
                  WHEN (food_clean.exercise = 3) THEN 'Once a week'
                  WHEN (food_clean.exercise = 4) THEN 'Sometimes'
                  WHEN (food_clean.exercise = 4) THEN 'Never'
            ELSE null END) exercise_desc,
            food_clean.fav_cuisine,
            food_clean.fav_cuisine_coded,
            (CASE WHEN (food_clean.fav_cuisine_coded = 0) THEN 'none'
                  WHEN (food_clean.fav_cuisine_coded = 1) THEN 'Italian/French/greek'
                  WHEN (food_clean.fav_cuisine_coded = 2) THEN 'Spanish/mexican'
                  WHEN (food_clean.fav_cuisine_coded = 3) THEN 'Arabic/Turkish'
                  WHEN (food_clean.fav_cuisine_coded = 4) THEN 'asian/chineses/thai/nepal'
                  WHEN (food_clean.fav_cuisine_coded = 5) THEN 'American'
                  WHEN (food_clean.fav_cuisine_coded = 6) THEN 'African'
                  WHEN (food_clean.fav_cuisine_coded = 7) THEN 'Jamaican'
                  WHEN (food_clean.fav_cuisine_coded = 8) THEN 'indian'
             ELSE null END) fav_cuisine_coded_desc,
            food_clean.fav_food,
            food_clean.food_childhood,
            food_clean.fruit_day,
            food_clean.greek_food,
            food_clean.healthy_feeling,
            food_clean.income,
            food_clean.indian_food,
            food_clean.italian_food,
            food_clean.marital_status,
            food_clean.nutritional_check,
            food_clean.on_off_campus,
            food_clean.parents_cook,
            food_clean.pay_meal_out,
            food_clean.persian_food,
            food_clean.self_perception_weight,
            food_clean.sports,
            food_clean.thai_food,
            food_clean.veggies_day,
            food_clean.vitamins,
            food_clean.weight

      FROM food_clean
      EMIT CHANGES;
