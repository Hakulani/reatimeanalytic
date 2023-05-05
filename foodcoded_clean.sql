CREATE STREAM foodcoded_clean WITH (kafka_topic = 'foodcoded_clean') AS 
      SELECT
            (CASE WHEN ((foodcoded.GPA = '') OR (foodcoded.GPA = 'Personal') OR (foodcoded.GPA = 'Unknown')) THEN null
             ELSE CAST(REGEXP_REPLACE(foodcoded.GPA, '[a-zA-Z]+', '') AS DOUBLE) END) GPA,
            (CASE WHEN (CAST(foodcoded.Gender AS VARCHAR) = '') THEN null ELSE foodcoded.Gender END) Gender,
            (CASE WHEN (CAST(foodcoded.calories_day AS VARCHAR) = '') THEN null ELSE foodcoded.calories_day END) calories_day,
            (CASE WHEN (CAST(foodcoded.comfort_food_reasons_coded1 AS VARCHAR) = '') THEN null ELSE foodcoded.comfort_food_reasons_coded1 END) comfort_food_reasons_coded,
            (CASE WHEN (CAST(foodcoded.cook AS VARCHAR) = '') THEN null ELSE foodcoded.cook END) cook,
            (CASE WHEN (CAST(foodcoded.cuisine AS VARCHAR) = '') THEN null ELSE foodcoded.cuisine END) cuisine,
            (CASE WHEN (CAST(foodcoded.diet_current_coded AS VARCHAR) = '') THEN null ELSE foodcoded.diet_current_coded END) diet_current_coded,
            (CASE WHEN (CAST(foodcoded.eating_out AS VARCHAR) = '') THEN null ELSE foodcoded.eating_out END) eating_out,
            (CASE WHEN (CAST(foodcoded.employment AS VARCHAR) = '') THEN null ELSE foodcoded.employment END) employment,
            (CASE WHEN (CAST(foodcoded.ethnic_food AS VARCHAR) = '') THEN null ELSE foodcoded.ethnic_food END) ethnic_food,
            (CASE WHEN (CAST(foodcoded.exercise AS VARCHAR) = '') THEN null ELSE foodcoded.exercise END) exercise,
            (CASE WHEN (CAST(foodcoded.fav_cuisine AS VARCHAR) = '') THEN null ELSE foodcoded.fav_cuisine END) fav_cuisine,
            (CASE WHEN (CAST(foodcoded.fav_cuisine_coded AS VARCHAR) = '') THEN null ELSE foodcoded.fav_cuisine_coded END) fav_cuisine_coded,
            (CASE WHEN (CAST(foodcoded.fav_food AS VARCHAR) = '') THEN null ELSE foodcoded.fav_food END) fav_food,
            (CASE WHEN (CAST(foodcoded.food_childhood AS VARCHAR) = '') THEN null ELSE foodcoded.food_childhood END) food_childhood,
            (CASE WHEN (CAST(foodcoded.fruit_day AS VARCHAR) = '') THEN null ELSE foodcoded.fruit_day END) fruit_day,
            (CASE WHEN (CAST(foodcoded.greek_food AS VARCHAR) = '') THEN null ELSE foodcoded.greek_food END) greek_food,
            (CASE WHEN (CAST(foodcoded.healthy_feeling AS VARCHAR) = '') THEN null ELSE foodcoded.healthy_feeling END) healthy_feeling,
            (CASE WHEN (CAST(foodcoded.income AS VARCHAR) = '') THEN null ELSE foodcoded.income END) income,
            (CASE WHEN (CAST(foodcoded.indian_food AS VARCHAR) = '') THEN null ELSE foodcoded.indian_food END) indian_food,
            (CASE WHEN (CAST(foodcoded.italian_food AS VARCHAR) = '') THEN null ELSE foodcoded.italian_food END) italian_food,
            (CASE WHEN (CAST(foodcoded.marital_status AS VARCHAR) = '') THEN null ELSE foodcoded.marital_status END) marital_status,
            (CASE WHEN (CAST(foodcoded.nutritional_check AS VARCHAR) = '') THEN null ELSE foodcoded.nutritional_check END) nutritional_check,
            (CASE WHEN (CAST(foodcoded.on_off_campus AS VARCHAR) = '') THEN null ELSE foodcoded.on_off_campus END) on_off_campus,
            (CASE WHEN (CAST(foodcoded.parents_cook AS VARCHAR) = '') THEN null ELSE foodcoded.parents_cook END) parents_cook,
            (CASE WHEN (CAST(foodcoded.pay_meal_out AS VARCHAR) = '') THEN null ELSE foodcoded.pay_meal_out END) pay_meal_out,
            (CASE WHEN (CAST(foodcoded.persian_food AS VARCHAR) = '') THEN null ELSE foodcoded.persian_food END) persian_food,
            (CASE WHEN (CAST(foodcoded.self_perception_weight AS VARCHAR) = '') THEN null ELSE foodcoded.self_perception_weight END) self_perception_weight,
            (CASE WHEN (CAST(foodcoded.sports AS VARCHAR) = '') THEN null ELSE foodcoded.sports END) sports,
            (CASE WHEN (CAST(foodcoded.thai_food AS VARCHAR) = '') THEN null ELSE foodcoded.thai_food END) thai_food,
            (CASE WHEN (CAST(foodcoded.veggies_day AS VARCHAR) = '') THEN null ELSE foodcoded.veggies_day END) veggies_day,
            (CASE WHEN (CAST(foodcoded.vitamins AS VARCHAR) = '') THEN null ELSE foodcoded.vitamins END) vitamins,
            (CASE WHEN (foodcoded.weight = '') THEN null
             ELSE CAST(REGEXP_REPLACE(foodcoded.weight, '[^0-9]+', '') AS INT) END) weight
      FROM foodcoded
      WHERE foodcoded.calories_day is not null
      EMIT CHANGES;
