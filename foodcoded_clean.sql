CREATE STREAM foodcoded_clean WITH (kafka_topic = 'foodcoded_clean') AS 
      SELECT
            GPA                           = CASE WHEN ((foodcoded.GPA = '') OR (foodcoded.GPA = 'Personal') OR (foodcoded.GPA = 'Unknown')) THEN null
                                            ELSE CAST(REGEXP_REPLACE(foodcoded.GPA, '[^0-9]+', '') AS FLOAT) END,
            Gender                        = CASE WHEN (foodcoded.Gender = '') THEN null ELSE foodcoded.Gender END,
            calories_day                  = CASE WHEN (foodcoded.calories_day = '') THEN null ELSE foodcoded.calories_day END,
            comfort_food_reasons_coded    = CASE WHEN (foodcoded.comfort_food_reasons_coded1 = '') THEN null ELSE foodcoded.comfort_food_reasons_coded1 END,
            cook                          = CASE WHEN (foodcoded.cook = '') THEN null ELSE foodcoded.cook END,
            cuisine                       = CASE WHEN (foodcoded.cuisine = '') THEN null ELSE foodcoded.cuisine END,
            diet_current_coded            = CASE WHEN (foodcoded.diet_current_coded = '') THEN null ELSE foodcoded.diet_current_coded END,
            eating_out                    = CASE WHEN (foodcoded.eating_out = '') THEN null ELSE foodcoded.eating_out END,
            employment                    = CASE WHEN (foodcoded.employment = '') THEN null ELSE foodcoded.employment END,
            ethnic_food                   = CASE WHEN (foodcoded.ethnic_food = '') THEN null ELSE foodcoded.ethnic_food END,
            exercise                      = CASE WHEN (foodcoded.exercise = '') THEN null ELSE foodcoded.exercise END,
            fav_cuisine                   = CASE WHEN (foodcoded.fav_cuisine = '') THEN null ELSE foodcoded.fav_cuisine END,
            fav_cuisine_coded             = CASE WHEN (foodcoded.fav_cuisine_coded = '') THEN null ELSE foodcoded.fav_cuisine_coded END,
            fav_food                      = CASE WHEN (foodcoded.fav_food = '') THEN null ELSE foodcoded.fav_food END,
            food_childhood                = CASE WHEN (foodcoded.food_childhood = '') THEN null ELSE foodcoded.food_childhood END,
            fruit_day                     = CASE WHEN (foodcoded.fruit_day = '') THEN null ELSE foodcoded.fruit_day END,
            greek_food                    = CASE WHEN (foodcoded.greek_food = '') THEN null ELSE foodcoded.greek_food END,
            healthy_feeling               = CASE WHEN (foodcoded.healthy_feeling = '') THEN null ELSE foodcoded.healthy_feeling END,
            income                        = CASE WHEN (foodcoded.income = '') THEN null ELSE foodcoded.income END,
            indian_food                   = CASE WHEN (foodcoded.indian_food = '') THEN null ELSE foodcoded.indian_food END,
            italian_food                  = CASE WHEN (foodcoded.italian_food = '') THEN null ELSE foodcoded.italian_food END,
            marital_status                = CASE WHEN (foodcoded.marital_status = '') THEN null ELSE foodcoded.marital_status END,
            nutritional_check             = CASE WHEN (foodcoded.nutritional_check = '') THEN null ELSE foodcoded.nutritional_check END,
            on_off_campus                 = CASE WHEN (foodcoded.on_off_campus = '') THEN null ELSE foodcoded.on_off_campus END,
            parents_cook                  = CASE WHEN (foodcoded.parents_cook = '') THEN null ELSE foodcoded.parents_cook END,
            pay_meal_out                  = CASE WHEN (foodcoded.pay_meal_out = '') THEN null ELSE foodcoded.pay_meal_out END,
            persian_food                  = CASE WHEN (foodcoded.persian_food = '') THEN null ELSE foodcoded.persian_food END,
            self_perception_weight        = CASE WHEN (foodcoded.self_perception_weight = '') THEN null ELSE foodcoded.self_perception_weight END,
            sports                        = CASE WHEN (foodcoded.sports = '') THEN null ELSE foodcoded.sports END,
            thai_food                     = CASE WHEN (foodcoded.thai_food = '') THEN null ELSE foodcoded.thai_food END,
            veggies_day                   = CASE WHEN (foodcoded.veggies_day = '') THEN null ELSE foodcoded.veggies_day END,
            vitamins                      = CASE WHEN (foodcoded.vitamins = '') THEN null ELSE foodcoded.vitamins END,
			waffle_calories				  = CASE WHEN (foodcoded.waffle_calories = '') THEN null ELSE foodcoded.waffle_calories END,
			turkey_calories				  = CASE WHEN (foodcoded.turkey_calories = '') THEN null ELSE foodcoded.turkey_calories END,			
            weight                        = CASE WHEN (foodcoded.weight = '') THEN null
                                            ELSE ELSE CAST(REGEXP_REPLACE(foodcoded.weight, '[^0-9]+', '') AS INT) END
      FROM foodcoded
	  WHERE calories_day is not NULL
      EMIT CHANGES;
