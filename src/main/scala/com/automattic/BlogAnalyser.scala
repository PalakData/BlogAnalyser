package com.automattic

import org.apache.spark.sql.SparkSession

object BlogAnalyser {

  /*
   since data will be huge , I am expecting this to be either stored in hdfs or cloud platform
   currently reading it from the local machine
  */

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Blog Analyser").master("local").config("spark.sql.warehouse.dir", "/test/automattic").getOrCreate()
    // args(0) -> path of the data: /Users/palakdata/Documents/test/automattic/posts.jsonl.gz
    val inputPath = args(0)
    val baseOutputPath = args(1)
    val blogs = spark.read.json(inputPath)


    /*
     ############# 1. What are the median and the mean numbers of likes per post in this data sample? #########
     - to elimiate duplicates likes per post per blog
     - post is associated with per blog, so mean and median will be calculated per post per blog
     - using window functions to calculate mean and median
     - median : approx median can be calculated through percentile_approx method. If we need to find the exact median then we can hae a separate sql or udf
     - for mean and medians :0 is considered in the values and eliminatinf 0 will change the values
     */
    val likes_per_post = blogs.select("blog_id", "post_id", "like_count").distinct()
    likes_per_post.createOrReplaceTempView("df_likes")

    val mean_median_likes_per_post = spark.sql("Select DISTINCT blog_id, post_id ,mean(like_count) over(partition by blog_id,post_id) as mean_likes, percentile_approx(like_count,0.5) over(partition by blog_id,post_id) as median_likes from df_likes")
    mean_median_likes_per_post.write.csv(baseOutputPath + "mean_median_likes_per_post")


    /*
    // ############ 2. What is the mean number of posts per author in this data sample? ##############
    - will select relevant columns from the entire dataframe
    - first we need to cound the no of posts per blog, so as to calculate the mean of posts per author
    - after calculating , we can use window funtion to get mean
    - used sub-queries here
    */
    val author_post = blogs.select("author_id", "author", "post_id", "blog_id").distinct()
    author_post.createOrReplaceTempView("df_author")
    val mean_posts_per_author = spark.sql("Select DISTINCT  author_id, author ,mean(no_of_posts) over(partition by author_id) as mean  from (Select DISTINCT author_id,author,count(post_id) over (partition by blog_id)no_of_posts from df_author)")
    mean_posts_per_author.write.csv(baseOutputPath + "mean_posts_per_author")


    /* #############  3. My favourite colour is red. How many of the posts in this sample mention it in their titles? What are the limitations of your approach to this question?   ##############
    - will check the presence of red
    - limitations:
        - languages are different, need to check in that language
        - red can be in negatiVe sense too like i donot like red which is not the objective
        - the expression will skip red. or ,red.. etc
        - right approach shud be -> some ML algo shud be used to train the model and learn eventually

     */
    val red_check = blogs.select("post_id", "title").distinct()
    red_check.createOrReplaceTempView("red_check")

    val red_present = spark.sql("Select * from red_check where lower(title) like '% red %'")
    red_present.write.csv(baseOutputPath + "red_present")





    /*
    ########   4. How many of the authors in this sample have not liked any of the posts in this sample? ######
      - first we need the list of likers_id -> we use explode to get it as a column
      - anti join with author_id to get only one who donot match
     */
    import org.apache.spark.sql.functions._
    val authors = blogs.select("author_id").dropDuplicates()
    val liker_ids = blogs.select(explode(col("liker_ids"))).dropDuplicates()
    val authors_with_no_post_liked = authors.join(liker_ids, authors("author_id") === liker_ids("col"), "left_anti")
    authors_with_no_post_liked.write.csv(baseOutputPath + "authors_with_no_post_liked")




    /* #############
    5. Which tools did you use to obtain these results and why did you choose them? How would you have approached this task if you had had a data set with a hundred times as many posts?
      ##########
      - used spark SQL as it is a parallel processing framework and is really fast and optimised than others
      - code implementation was done in Intellij
      - If the data was hundred times, I would have made sure the data is distributed and processing is done parallely through the same frameworks. Instead of spark SQL, dataframe APIs can also be used.
     */
  }
}
