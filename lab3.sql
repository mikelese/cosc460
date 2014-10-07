/* COSC 460 Fall 2014, Lab 3 */

/* These set the output format.  Please be sure to leave these settings as is. */
.header OFF
.mode list 

/* For each of the queries below, put your SQL in the place indicated by the comment.  
   Be sure to have all the requested columns in your answer, in the order they are 
   listed in the question - and be sure to sort things where the question requires 
   them to be sorted, and eliminate duplicates where the question requires that.   
   I will grade the assignment by running the queries on a test database and 
   eyeballing the SQL queries where necessary.  I won't grade on SQL style, but 
   we also won't give partial credit for any individual question - so you should be 
   confident that your query works. In particular, your output should match 
   the example output.
*/

/* Q1 -  Find the titles of all movies directed by Steven Spielberg.  */
select " ";
select "Q1";

/* Put your SQL for Q1 below */
select title from Movie where director="Steven Spielberg";

/* Q2 -  Find all years that have a movie that received a rating of 4 or 5, 
         and sort them in increasing order.             
*/
select " ";
select "Q2";

/* Put your SQL for Q2 below */
select distinct(m.year) from Movie as m,Rating as r where m.mID == r.mID AND r.stars >=4 ORDER BY m.year;

/* Q3 -  Find the titles of all movies that have no ratings.
*/
select " ";
select "Q3";

/* Put your SQL for Q3 below */
select distinct(m.title) from Movie as m, Rating as r where m.mID NOT IN (select mID from Rating);

/* Q4 -  Write a query to return the ratings data in a more 
         readable format: reviewer name, movie title, stars, and ratingDate. 
         Also, sort the data, first by reviewer name, then by movie title, 
         and lastly by number of stars, all in ascending order.
*/
select " ";
select "Q4";

/* Put your SQL for Q4 below */
select rev.name, m.title, r.stars, r.ratingDate from Movie as m, Rating as r, Reviewer as rev where r.rID == rev.rID AND m.mID == r.mID ORDER BY rev.name, m.title, r.stars;

/* Q5 -  For all cases where the same reviewer rated the same movie twice 
         and gave it a higher rating the second time, return the reviewer's 
         name and the title of the movie.
*/
select " ";
select "Q5";

/* Put your SQL for Q5 below */
SELECT m.title, rev.name from Rating AS r1, Rating AS r2, Movie AS m, Reviewer AS rev WHERE r1.stars>r2.stars AND r1.mID == r2.mID AND r1.rID == r2.rID AND r1.mID == m.mID AND r1.rID==rev.rID AND r1.ratingDate > r2.ratingDate;

/* Q6 - For each movie that has at least one rating, find the highest number 
        of stars that movie received. Return the movie title and number of 
        stars. Sort by movie title. 
*/
select " ";
select "Q6";

/* Put your SQL for Q6 below */
select m.title, r1.stars from Movie AS m, Rating as r1, Rating as r2 where r1.stars > r2.stars AND r1.mID == r2.mID AND r1.mID == m.mID GROUP BY r1.mID ORDER BY m.title;

/* Q7 - For each movie, the title along with the number of ratings it has 
        received.  Your result should include those movies that have zero ratings.                                                                 
*/
select " ";
select "Q7";

/* Put your SQL for Q7 below */
select m.title, totalTable.c from (select mID, count(*) as c from Rating group by mID) as totalTable, Movie m where totalTable.mID ==m.mID UNION SELECT m.title, 0 from Movie m where m.mID NOT IN (select mID from Rating) ORDER BY m.title;


/* Q8 - For each movie that has at least one rating, return the title and the 
        'rating spread', that is, the difference between highest and lowest 
        ratings given to that movie. Sort by rating spread from highest to 
        lowest, then by movie title alphabetically.   
*/
select " ";
select "Q8";

/* Put your SQL for Q8 below */
select m.title, spreads.c from (select mID, max(stars)-min(stars) as c from Rating group by mID) as spreads, Movie m where spreads.mID==m.mID order by -spreads.c,m.title;


/* Q9 -  Find the difference between the average rating of movies released before 
         1980 and the average rating of movies released after 1980. (Make sure to 
         calculate the average rating for each movie, then the average of those 
         averages for movies before 1980 and movies after. Don't just calculate 
         the overall average rating before and after 1980.)  
*/
select " ";
select "Q9";

/* Put your SQL for Q9 below */
select avg(pre1980.a)-avg(post1980.a) from (select avg(stars) as a from Rating r, Movie m where m.year<=1980 and m.mID == r.mID group by r.mID) as pre1980, (select avg(stars) as a from Rating r, Movie m where m.year>1980 and m.mID=r.mID group by r.mID) as post1980;

/* Q10 - For each director, return the director's name together with the title(s) 
         of the movie(s) they directed that received the highest rating among all 
         of their movies, and the value of that rating. 
*/
select " ";
select "Q10";

/* Put your SQL for Q10 below */
select distinct(m.director), m.title, dirstar.stars from (select m.director as dir, max(r.stars) as stars from Movie m, Rating r where m.mID == r.mID group by m.director) as dirstar, Movie m, rating r where m.mID==r.mID and m.director==dirstar.dir and dirstar.stars==r.stars;











