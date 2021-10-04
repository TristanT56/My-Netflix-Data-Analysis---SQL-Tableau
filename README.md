# My Netflix Data Analysis - SQL + Tableau


### Table of Contents

* [Introduction](#chapter1)


* [A) Cleaning & Peparation](#chapter2)
    * [Connecting Postgresql to this Jupyter notebook](#section_2_1)
    * [Selecting and reorganising my columns](#section_2_2)
    * [Cleaning my columns](#section_2_3) 
        * [Duration column: dropping the wrong records](#section_2_3_1)
        * [Country column: simplifying the country names](#section_2_3_2)
        * [Device column: reorganising the categories](#section_2_3_3)
        * [Title column: automating the extraction of parts (Series/Season/Episode) for each title (with different formats)](#section_2_3_4)
            * [Selecting and extracting the series titles](#section_2_3_4_1) 
            * [Breaking down the parts of the title using STRING_TO_ARRAY()](#section_2_3_4_2) 
            * [Finding a way to automate the selection of the part with the season](#section_2_3_4_3) 
            * [Gathering all the parts of the title/season/episode with ARRAY_AGG() and ARRAY_TO_STRING()](#section_2_3_4_4) 
            * [Inserting the new series/season/episode titles into our original table](#section_2_3_4_5) 
    * [Generating a serie of dates](#section_2_4)      



* [B) Analysis](#chapter3)
    * [Top 5 Series](#section_3_1)
    * [Top 5 Movies](#section_3_2)
    * [Series VS Movies](#section_3_3)
    * [My Netflix consumption per month](#section_3_4)
    * [Device analysis: On what do I watch Netflix?](#section_3_5)
    * [Country analysis: Where in the world did I watch Netflix?](#section_3_6)


* [C) Visualisation with Tableau](#chapter4)


* [Conclusion](#chapter5)



## Introduction <a class="anchor" id="chapter1"></a>

The main goal of this project is to practice my SQL skills and to introduce you to another part of myself which is my movie and serie tastes.
That is why I will conduct this project only with SQL but I will, at the end, also include a visualisation made with Tableau. 

In this project, I will analyse my Netflix data from 2019 to July 2021. First, I will clean and prepare the data and then I'll analyse my Netflix data. 

The objectives are :

   - To know my top 5 series and my top 5 movies.
   - To compare my consumption of series vs movies (overall consumption but also by year).
   - To know the months when I consume Netflix the most.
   - To analyse on which device I watch Netflix in general but also by hour of the day.
   - To analyse where in the world I watched Netflix from 2019 to 2021.


About the data:

There is only one dataset, it is my historical Netflix data (in csv format). I got it by requesting it from my Netflix account.

## A) Cleaning & Peparation <a class="anchor" id="chapter2"></a>

### 1 - Connecting Postgresql to this Jupyter notebook: <a class="anchor" id="section_2_1"></a>

Let's connect postgresql (pgadmin 4) and this jupyter notebook tobe able to write SQL queries directly on this notebook: 


```python
%load_ext sql
%sql postgresql://postgres:7897@localhost:5433/N1
```




    'Connected: postgres@N1'



### 2 - Selecting and reorganising my columns: <a class="anchor" id="section_2_2"></a>

Ok we are connected to my postgresql database.

I have already imported my netflix history data (csv file), that I requested my netflix account, into pgAdmin 4 (postgresql). 

Let's open it here, after importation, without any changes:


```sql
%%sql 

SELECT *
FROM netflix_file
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>Profile_Name</th>
            <th>Start_Time</th>
            <th>Duration</th>
            <th>Attributes</th>
            <th>Title</th>
            <th>Supplemental_Video_Type</th>
            <th>Device_Type</th>
            <th>Bookmark</th>
            <th>Latest_Bookmark</th>
            <th>Country</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Joco</td>
            <td>2021-07-11 18:04:01</td>
            <td>00:00:06</td>
            <td>Autoplayed: user action: None; </td>
            <td>Bande-annonce : Notre planète a ses limites : L’alerte de la science</td>
            <td>TRAILER</td>
            <td>Chrome PC (Cadmium)</td>
            <td>00:00:06</td>
            <td>00:00:06</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>Joco</td>
            <td>2021-07-11 17:48:18</td>
            <td>00:01:25</td>
            <td>Autoplayed: user action: None; </td>
            <td>Génération 56k : Saison 1 (Bande-annonce)</td>
            <td>TRAILER</td>
            <td>Chrome PC (Cadmium)</td>
            <td>00:01:25</td>
            <td>00:01:25</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>Joco</td>
            <td>2021-07-08 11:23:03</td>
            <td>00:00:15</td>
            <td>Autoplayed: user action: None; </td>
            <td>Friends: Saison 3: Celui qui courait deux lièvres (Épisode 20)</td>
            <td>None</td>
            <td>Chrome PC (Cadmium)</td>
            <td>00:00:16</td>
            <td>00:00:16</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>Joco</td>
            <td>2021-07-08 10:56:52</td>
            <td>00:22:40</td>
            <td>None</td>
            <td>Friends: Saison 3: Celui qui avait un T-shirt trop petit (Épisode 19)</td>
            <td>None</td>
            <td>Chrome PC (Cadmium)</td>
            <td>00:22:40</td>
            <td>00:22:40</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>Joco</td>
            <td>2021-07-05 20:16:50</td>
            <td>00:00:05</td>
            <td>Autoplayed: user action: None; </td>
            <td>Modern Family: Season 1_hook_primary_16x9</td>
            <td>HOOK</td>
            <td>Chrome PC (Cadmium)</td>
            <td>00:00:05</td>
            <td>00:00:05</td>
            <td>FR (France)</td>
        </tr>
    </tbody>
</table>



I am sharing my Netflix account with my family, so let’s only keep my Netflix (Profile_Name = ‘TT’).
We will create a new table to keep the original table.


```sql
%%sql 

SELECT *
INTO TABLE netflix_tt
FROM netflix_file
WHERE "Profile_Name" = 'TT'
ORDER BY "Start_Time" DESC;

SELECT * 
FROM netflix_tt
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    2563 rows affected.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>Profile_Name</th>
            <th>Start_Time</th>
            <th>Duration</th>
            <th>Attributes</th>
            <th>Title</th>
            <th>Supplemental_Video_Type</th>
            <th>Device_Type</th>
            <th>Bookmark</th>
            <th>Latest_Bookmark</th>
            <th>Country</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>TT</td>
            <td>2021-07-14 08:14:43</td>
            <td>00:00:58</td>
            <td>Autoplayed: user action: None; </td>
            <td>Generation 56K: Season 1 (Trailer)</td>
            <td>TRAILER</td>
            <td>Chrome PC (Cadmium)</td>
            <td>00:00:58</td>
            <td>00:00:58</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-13 16:38:15</td>
            <td>00:00:05</td>
            <td>Autoplayed: user action: None; </td>
            <td>Major Grom_hook_01_16x9</td>
            <td>HOOK</td>
            <td>Chrome PC (Cadmium)</td>
            <td>00:00:05</td>
            <td>00:00:05</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-13 16:36:45</td>
            <td>00:01:19</td>
            <td>Autoplayed: user action: None; </td>
            <td>Season 2 Trailer: Biohackers</td>
            <td>TRAILER</td>
            <td>Chrome PC (Cadmium)</td>
            <td>00:01:19</td>
            <td>00:01:19</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-13 14:53:45</td>
            <td>01:34:18</td>
            <td>None</td>
            <td>How I Became a Superhero</td>
            <td>None</td>
            <td>Chrome PC (Cadmium)</td>
            <td>01:34:22</td>
            <td>01:34:22</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-13 14:53:00</td>
            <td>00:00:07</td>
            <td>Autoplayed: user action: None; </td>
            <td>Season 2 Trailer: Biohackers</td>
            <td>TRAILER</td>
            <td>Chrome PC (Cadmium)</td>
            <td>00:00:07</td>
            <td>Not latest view</td>
            <td>FR (France)</td>
        </tr>
    </tbody>
</table>



Let’s keep only the NULL values of Supplemental_Video_Type to keep only movie and series because the not null values are TRAILER or something similar (not real movie or series).
See below:


```sql
%%sql 

SELECT "Supplemental_Video_Type"
FROM netflix_tt
GROUP BY "Supplemental_Video_Type";
```

     * postgresql://postgres:***@localhost:5433/N1
    7 rows affected.
    




<table>
    <thead>
        <tr>
            <th>Supplemental_Video_Type</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>None</td>
        </tr>
        <tr>
            <td>TEASER_TRAILER</td>
        </tr>
        <tr>
            <td>PREVIEW</td>
        </tr>
        <tr>
            <td>TRAILER</td>
        </tr>
        <tr>
            <td>HOOK</td>
        </tr>
        <tr>
            <td>RECAP</td>
        </tr>
        <tr>
            <td>PROMOTIONAL</td>
        </tr>
    </tbody>
</table>




```sql
%%sql 

DELETE FROM netflix_tt
WHERE "Supplemental_Video_Type" IS NOT NULL;

SELECT "Supplemental_Video_Type"
FROM netflix_tt
GROUP BY "Supplemental_Video_Type";
```

     * postgresql://postgres:***@localhost:5433/N1
    420 rows affected.
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>Supplemental_Video_Type</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>None</td>
        </tr>
    </tbody>
</table>



Let's keep only columns that interest us: Profile_Name, Start_Time, Duration, Title, Device_Type and Country.


```sql
%%sql 

ALTER TABLE netflix_tt
DROP COLUMN "Attributes",
DROP COLUMN "Supplemental_Video_Type",
DROP COLUMN "Bookmark",
DROP COLUMN "Latest_Bookmark";

SELECT *
FROM netflix_tt
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>Profile_Name</th>
            <th>Start_Time</th>
            <th>Duration</th>
            <th>Title</th>
            <th>Device_Type</th>
            <th>Country</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>TT</td>
            <td>2021-07-13 14:53:45</td>
            <td>01:34:18</td>
            <td>How I Became a Superhero</td>
            <td>Chrome PC (Cadmium)</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 11:09:59</td>
            <td>00:00:58</td>
            <td>Human: The World Within: Season 1: React (Episode 1)</td>
            <td>DefaultWidevineAndroidPhone</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 11:08:41</td>
            <td>00:00:06</td>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>DefaultWidevineAndroidPhone</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 10:15:31</td>
            <td>00:35:57</td>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>DefaultWidevineAndroidPhone</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 08:13:56</td>
            <td>00:10:34</td>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>DefaultWidevineAndroidPhone</td>
            <td>FR (France)</td>
        </tr>
    </tbody>
</table>



I am using PostgreSQL and if my columns have a name with a capital letter, I have to add " ", this is annoying, so I will change all column names to lower case and simple names.


```sql
%%sql 

ALTER TABLE netflix_tt
RENAME COLUMN "Profile_Name" TO profile;

ALTER TABLE netflix_tt
RENAME COLUMN "Start_Time" TO datetime;

ALTER TABLE netflix_tt
RENAME COLUMN "Duration" TO duration;

ALTER TABLE netflix_tt
RENAME COLUMN "Title" TO title;

ALTER TABLE netflix_tt
RENAME COLUMN "Device_Type" TO device;

ALTER TABLE netflix_tt
RENAME COLUMN "Country" TO country;

SELECT *
FROM netflix_tt
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    Done.
    Done.
    Done.
    Done.
    Done.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>profile</th>
            <th>datetime</th>
            <th>duration</th>
            <th>title</th>
            <th>device</th>
            <th>country</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>TT</td>
            <td>2021-07-13 14:53:45</td>
            <td>01:34:18</td>
            <td>How I Became a Superhero</td>
            <td>Chrome PC (Cadmium)</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 11:09:59</td>
            <td>00:00:58</td>
            <td>Human: The World Within: Season 1: React (Episode 1)</td>
            <td>DefaultWidevineAndroidPhone</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 11:08:41</td>
            <td>00:00:06</td>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>DefaultWidevineAndroidPhone</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 10:15:31</td>
            <td>00:35:57</td>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>DefaultWidevineAndroidPhone</td>
            <td>FR (France)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 08:13:56</td>
            <td>00:10:34</td>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>DefaultWidevineAndroidPhone</td>
            <td>FR (France)</td>
        </tr>
    </tbody>
</table>



I can also lower all the rows but it will help me later to have some capital letter.In the title column, for example, we will separate the name of the series and the name of the episodes.

Now, let's see if everything is ok with the data type of my columns:


```sql
%%sql 

SELECT COLUMN_NAME,
       DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME='netflix_tt';
```

     * postgresql://postgres:***@localhost:5433/N1
    6 rows affected.
    




<table>
    <thead>
        <tr>
            <th>column_name</th>
            <th>data_type</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>datetime</td>
            <td>timestamp without time zone</td>
        </tr>
        <tr>
            <td>duration</td>
            <td>time without time zone</td>
        </tr>
        <tr>
            <td>profile</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>title</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>device</td>
            <td>character varying</td>
        </tr>
        <tr>
            <td>country</td>
            <td>character varying</td>
        </tr>
    </tbody>
</table>



Ok all good.

Let's clean & prepare the columns:

### 3 - Cleaning my columns: <a class="anchor" id="section_2_3"></a>

#### 1) Duration column: dropping the wrong records. <a class="anchor" id="section_2_3_1"></a>

Let's explore and clean up the duration column.


```sql
%%sql 

SELECT COUNT(*) FILTER (WHERE duration < '00:01:00') AS lessthan1min,
    COUNT(*) FILTER (WHERE duration >= '00:01:00' AND duration < '00:05:00') AS between1and5min,
    COUNT(*) FILTER (WHERE duration >= '00:05:00' AND duration < '00:010:00') AS between5and10min,
    COUNT(*) FILTER (WHERE duration >= '00:10:00' AND duration < '00:20:00') AS between10and20min,
    COUNT(*) FILTER (WHERE duration >= '00:20:00') AS morethan20min
FROM netflix_tt;
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>lessthan1min</th>
            <th>between1and5min</th>
            <th>between5and10min</th>
            <th>between10and20min</th>
            <th>morethan20min</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>276</td>
            <td>160</td>
            <td>120</td>
            <td>245</td>
            <td>1342</td>
        </tr>
    </tbody>
</table>



Let's say that less than 1 minute is an error and is not counted and more than one minute is ok (ex: when you watch 4 minutes while waiting for your train, you stop to get on the train and watch it again).


```sql
%%sql 

DELETE FROM netflix_tt
WHERE duration < '00:01:00';

SELECT COUNT(*) FILTER (WHERE duration < '00:01:00') AS lessthan1min,
    COUNT(*) FILTER (WHERE duration >= '00:01:00' AND duration < '00:05:00') AS between1and5min,
    COUNT(*) FILTER (WHERE duration >= '00:05:00' AND duration < '00:010:00') AS between5and10min,
    COUNT(*) FILTER (WHERE duration >= '00:10:00' AND duration < '00:20:00') AS between10and20min,
    COUNT(*) FILTER (WHERE duration >= '00:20:00') AS morethan20min
FROM netflix_tt;
```

     * postgresql://postgres:***@localhost:5433/N1
    276 rows affected.
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>lessthan1min</th>
            <th>between1and5min</th>
            <th>between5and10min</th>
            <th>between10and20min</th>
            <th>morethan20min</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>0</td>
            <td>160</td>
            <td>120</td>
            <td>245</td>
            <td>1342</td>
        </tr>
    </tbody>
</table>



#### 2) Country column: simplifying the country names. <a class="anchor" id="section_2_3_2"></a>

Let's explore and clean up the country column.


```sql
%%sql 

SELECT country
FROM netflix_tt
GROUP BY country;
```

     * postgresql://postgres:***@localhost:5433/N1
    4 rows affected.
    




<table>
    <thead>
        <tr>
            <th>country</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>KH (Cambodia)</td>
        </tr>
        <tr>
            <td>GB (United Kingdom)</td>
        </tr>
        <tr>
            <td>CN (China)</td>
        </tr>
        <tr>
            <td>FR (France)</td>
        </tr>
    </tbody>
</table>



OK you can see that I'm a traveller and that doesn't stop me from watching Netflix... 
(I don't remember being in China in the last few years. I hope some of them are not hackers from another country... I'll analyse that later)

Let's just keep the country name:


```sql
%%sql 

UPDATE netflix_tt
SET country = SUBSTRING(country from '\((.+)\)');

SELECT country
FROM netflix_tt
GROUP BY country;
```

     * postgresql://postgres:***@localhost:5433/N1
    1867 rows affected.
    4 rows affected.
    




<table>
    <thead>
        <tr>
            <th>country</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Cambodia</td>
        </tr>
        <tr>
            <td>France</td>
        </tr>
        <tr>
            <td>China</td>
        </tr>
        <tr>
            <td>United Kingdom</td>
        </tr>
    </tbody>
</table>



#### 3) Device column: reorganising the categories. <a class="anchor" id="section_2_3_3"></a>

Let's explore and clean up the device column.


```sql
%%sql 

SELECT device
FROM netflix_tt
GROUP BY device;
```

     * postgresql://postgres:***@localhost:5433/N1
    4 rows affected.
    




<table>
    <thead>
        <tr>
            <th>device</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>DefaultWidevineAndroidPhone</td>
        </tr>
        <tr>
            <td>Sony PS4</td>
        </tr>
        <tr>
            <td>Android DefaultWidevineL3Phone Android Phone</td>
        </tr>
        <tr>
            <td>Chrome PC (Cadmium)</td>
        </tr>
    </tbody>
</table>



I changed my phone around december 2020, that's why we can find two android phones. Then there's my computer and my ps4.

Let's change that to phone/computer/ps4.


```sql
%%sql 

SELECT device,
    CASE WHEN device LIKE '%Android%' THEN 'Phone'
         WHEN device LIKE '%PC%' THEN 'Computer'
         WHEN device LIKE '%PS4%' THEN 'PS4'
    END AS new_name
FROM netflix_tt
GROUP BY device;
```

     * postgresql://postgres:***@localhost:5433/N1
    4 rows affected.
    




<table>
    <thead>
        <tr>
            <th>device</th>
            <th>new_name</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>DefaultWidevineAndroidPhone</td>
            <td>Phone</td>
        </tr>
        <tr>
            <td>Sony PS4</td>
            <td>PS4</td>
        </tr>
        <tr>
            <td>Android DefaultWidevineL3Phone Android Phone</td>
            <td>Phone</td>
        </tr>
        <tr>
            <td>Chrome PC (Cadmium)</td>
            <td>Computer</td>
        </tr>
    </tbody>
</table>




```sql
%%sql 

UPDATE netflix_tt
SET device = CASE WHEN device LIKE '%Android%' THEN 'Phone'
         WHEN device LIKE '%PC%' THEN 'Computer'
         WHEN device LIKE '%PS4%' THEN 'PS4'
                END;

SELECT device
FROM netflix_tt
GROUP BY device;
```

     * postgresql://postgres:***@localhost:5433/N1
    1867 rows affected.
    3 rows affected.
    




<table>
    <thead>
        <tr>
            <th>device</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>PS4</td>
        </tr>
        <tr>
            <td>Phone</td>
        </tr>
        <tr>
            <td>Computer</td>
        </tr>
    </tbody>
</table>



#### 4) Title column: automating the extraction of parts (Series/Season/Episode) for each title (with different formats). <a class="anchor" id="section_2_3_4"></a>
    
Let's explore and clean up the title column.


```sql
%%sql 

SELECT title
FROM netflix_tt
GROUP BY title
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Rick and Morty: Season 4: Edge of Tomorty: Rick Die Rickpeat (Episode 1)</td>
        </tr>
        <tr>
            <td>Fargo: Season 1: The Crocodile&#x27;s Dilemma (Episode 1)</td>
        </tr>
        <tr>
            <td>Peaky Blinders: Season 2: Episode 4</td>
        </tr>
        <tr>
            <td>Eden</td>
        </tr>
        <tr>
            <td>Suits: Season 3: Conflict of Interest (Episode 4)</td>
        </tr>
    </tbody>
</table>



We can see that for a film, like "Eden" (excellent film by the way!) the title is ok but for a series, there is the title of the series, the season and the episode in the same string... not easy to analyse. Let's reorganise this.

##### 4.1) Selecting and extracting the series titles. <a class="anchor" id="section_2_3_4_1"></a>

We have to make sure we select all the series, so let's try to find a way to select them all.
(Also because I'm French, I wonder if "episode" couldn't have been written "épisode").



```sql
%%sql 

SELECT COUNT(*) FILTER(WHERE title LIKE '%Episode%') AS "Episode",
    COUNT(*) FILTER(WHERE title LIKE '%Épisode%') AS "Épisode",
    COUNT(*) FILTER(WHERE title LIKE '%episode%') AS "Épisode",
    COUNT(*) FILTER(WHERE title LIKE '%épisode%') AS "Épisode"
FROM netflix_tt;
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>Episode</th>
            <th>Épisode</th>
            <th>Épisode_1</th>
            <th>Épisode_2</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1630</td>
            <td>1</td>
            <td>0</td>
            <td>0</td>
        </tr>
    </tbody>
</table>




```sql
%%sql 

SELECT title
FROM netflix_tt
WHERE title LIKE '%Épisode%';
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Huge in France: Season 1: Épisode Un (Episode 1)</td>
        </tr>
    </tbody>
</table>



Ok "Épisode" is just in the name of the episode title, so there is no problem. We can use "Episode" to select all series.

Let's create a new 'type' column to sort the series and films: 


```sql
%%sql 

ALTER TABLE netflix_tt
ADD COLUMN type character varying;

UPDATE netflix_tt
SET type = CASE WHEN title LIKE '%Episode%' THEN 'Serie'
        WHEN title NOT LIKE '%Episode%' THEN 'Movie'
        END;

SELECT title, type
FROM netflix_tt
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    1867 rows affected.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
            <th>type</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>How I Became a Superhero</td>
            <td>Movie</td>
        </tr>
        <tr>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>Serie</td>
        </tr>
        <tr>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>Serie</td>
        </tr>
        <tr>
            <td>Sweet Tooth: Season 1: When Pubba Met Birdie (Episode 7)</td>
            <td>Serie</td>
        </tr>
        <tr>
            <td>Sweet Tooth: Season 1: Stranger Danger on a Train (Episode 6)</td>
            <td>Serie</td>
        </tr>
    </tbody>
</table>



Now let’s reorganise the serie titles. We will create a view to select only the series:


```sql
%%sql 

CREATE VIEW v1 AS 

WITH a AS (SELECT DISTINCT title
           FROM netflix_tt
           WHERE type = 'Serie')

SELECT ROW_NUMBER()OVER(ORDER BY title) AS num_title, *
FROM a;

SELECT *
FROM v1
LIMIT 5;

```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>num_title</th>
            <th>title</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
        </tr>
        <tr>
            <td>2</td>
            <td>3%: Season 1: Chapter 02: Coins (Episode 2)</td>
        </tr>
        <tr>
            <td>3</td>
            <td>3%: Season 1: Chapter 03: Corridor (Episode 3)</td>
        </tr>
        <tr>
            <td>4</td>
            <td>3%: Season 1: Chapter 04: Gateway (Episode 4)</td>
        </tr>
        <tr>
            <td>5</td>
            <td>3%: Season 1: Chapter 05: Water (Episode 5)</td>
        </tr>
    </tbody>
</table>



Quick verification:


```sql
%%sql 

SELECT COUNT(DISTINCT title)
FROM netflix_tt
WHERE type = 'Serie';
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1158</td>
        </tr>
    </tbody>
</table>




```sql
%%sql 

SELECT COUNT(*)
FROM v1;
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1158</td>
        </tr>
    </tbody>
</table>



Ok we're good.

##### 4.2) Breaking down the parts of the title using STRING_TO_ARRAY(). <a class="anchor" id="section_2_3_4_2"></a>

Let's break the title into parts with the delimiter ': ' using string_to_array() and then unnest it with ordinality. Finally, save the result in a new v2 view.


```sql
%%sql 

CREATE VIEW v2 AS 

SELECT v1.num_title, v1.title, a.title_part, a.num_part
FROM v1, UNNEST(STRING_TO_ARRAY(v1.title, ':')) WITH ORDINALITY a(title_part, num_part)
ORDER BY v1.num_title, a.num_part;

SELECT *
FROM v2
LIMIT 5;

```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>num_title</th>
            <th>title</th>
            <th>title_part</th>
            <th>num_part</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td>3%</td>
            <td>1</td>
        </tr>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td> Season 1</td>
            <td>2</td>
        </tr>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td> Chapter 01</td>
            <td>3</td>
        </tr>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td> Cubes (Episode 1)</td>
            <td>4</td>
        </tr>
        <tr>
            <td>2</td>
            <td>3%: Season 1: Chapter 02: Coins (Episode 2)</td>
            <td>3%</td>
            <td>1</td>
        </tr>
    </tbody>
</table>



Perfect. I want now to know the total number of parts for each title:


```sql
%%sql 

CREATE VIEW v3 AS 

SELECT *, MAX(num_part) OVER(PARTITION BY num_title) AS total_parts
FROM v2
ORDER BY num_title, num_part;

SELECT *
FROM v3
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>num_title</th>
            <th>title</th>
            <th>title_part</th>
            <th>num_part</th>
            <th>total_parts</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td>3%</td>
            <td>1</td>
            <td>4</td>
        </tr>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td> Season 1</td>
            <td>2</td>
            <td>4</td>
        </tr>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td> Chapter 01</td>
            <td>3</td>
            <td>4</td>
        </tr>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td> Cubes (Episode 1)</td>
            <td>4</td>
            <td>4</td>
        </tr>
        <tr>
            <td>2</td>
            <td>3%: Season 1: Chapter 02: Coins (Episode 2)</td>
            <td>3%</td>
            <td>1</td>
            <td>4</td>
        </tr>
    </tbody>
</table>




```sql
%%sql 

SELECT total_parts, COUNT(*) AS row_num
FROM v3
GROUP BY total_parts
ORDER BY row_num DESC;
```

     * postgresql://postgres:***@localhost:5433/N1
    2 rows affected.
    




<table>
    <thead>
        <tr>
            <th>total_parts</th>
            <th>row_num</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>3</td>
            <td>3063</td>
        </tr>
        <tr>
            <td>4</td>
            <td>548</td>
        </tr>
    </tbody>
</table>



So we have mainly records of 3-part titles but also some with 4 parts.


##### 4.3) Finding a way to automate the selection of the part with the season. <a class="anchor" id="section_2_3_4_3"></a>

Let's find a way to select the part with the season.

1) For titles with only 3 parts, we have 1021 distinct titles (see below). For those ones, it is always the same pattern: Title / Season / Episode. The season part is part 2.



```sql
%%sql 

SELECT title, COUNT(*) OVER() AS total_distinct_titles
FROM v3
WHERE total_parts = 3
GROUP BY title
ORDER BY RANDOM()
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
            <th>total_distinct_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Peaky Blinders: Season 3: Episode 4</td>
            <td>1021</td>
        </tr>
        <tr>
            <td>How I Met Your Mother: Season 8: The Time Travelers (Episode 20)</td>
            <td>1021</td>
        </tr>
        <tr>
            <td>Z Nation: Season 4: Crisis of Faith (Episode 8)</td>
            <td>1021</td>
        </tr>
        <tr>
            <td>White Gold: Season 1: The Secret of Sales (Episode 6)</td>
            <td>1021</td>
        </tr>
        <tr>
            <td>H: Season 1: The Best Friend (Episode 4)</td>
            <td>1021</td>
        </tr>
    </tbody>
</table>



2) For the 4-part titles: we have 137 distinct titles (see below). The season part can be in part 2 or part 3.


```sql
%%sql 

SELECT title, COUNT(*) OVER() AS total_distinct_titles
FROM v3
WHERE total_parts = 4
GROUP BY title
ORDER BY RANDOM()
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
            <th>total_distinct_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Stranger Things: Stranger Things 3: Chapter One: Suzie, Do You Copy? (Episode 1)</td>
            <td>137</td>
        </tr>
        <tr>
            <td>3%: Season 4: Chapter 07: Sun (Episode 7)</td>
            <td>137</td>
        </tr>
        <tr>
            <td>Formula 1: Drive to Survive: Season 3: Man On Fire (Episode 9)</td>
            <td>137</td>
        </tr>
        <tr>
            <td>Warrior Nun: Season 1: Proverbs 14:1 (Episode 8)</td>
            <td>137</td>
        </tr>
        <tr>
            <td>3%: Season 2: Chapter 01: Mirror (Episode 1)</td>
            <td>137</td>
        </tr>
    </tbody>
</table>



We can therefore define part 2 as the season part for titles with only 3 parts, but we need to find a way to automate the selection of the season part for titles with 4 parts.

So we have 137 distinct titles with 4 parts and we can observe in the titles that the word "Season" is (obviously) often used for the season part.

Let's see if any titles remain if we exclude titles with the word "Season": 


```sql
%%sql 

SELECT title, COUNT(*) OVER() AS total_distinct_titles
FROM v3
WHERE total_parts = 4
AND title NOT LIKE '%Season%'
GROUP BY title
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
            <th>total_distinct_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Criminal: France: Limited Series: Caroline (Episode 2)</td>
            <td>22</td>
        </tr>
        <tr>
            <td>Criminal: France: Limited Series: Émilie (Episode 1)</td>
            <td>22</td>
        </tr>
        <tr>
            <td>Criminal: France: Limited Series: Jérôme (Episode 3)</td>
            <td>22</td>
        </tr>
        <tr>
            <td>Inside Bill&#x27;s Brain: Decoding Bill Gates: Limited Series: Part 1 (Episode 1)</td>
            <td>22</td>
        </tr>
        <tr>
            <td>November 13: Attack on Paris: Limited Series: Episode 1</td>
            <td>22</td>
        </tr>
    </tbody>
</table>



This leaves 22 distinct titles and we can see that the word 'Serie' is also used for the season part. 

Let's look at the result if we also exclude the titles using the word 'Serie':


```sql
%%sql 

SELECT title, COUNT(*) OVER() AS total_distinct_titles
FROM v3
WHERE total_parts = 4
AND title NOT LIKE '%Season%'
AND title NOT LIKE '%Serie%'
GROUP BY title
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
            <th>total_distinct_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Roman Empire: Commodus: Reign of Blood: Born in the Purple (Episode 1)</td>
            <td>9</td>
        </tr>
        <tr>
            <td>Stranger Things: Stranger Things 3: Chapter Eight: The Battle of Starcourt (Episode 8)</td>
            <td>9</td>
        </tr>
        <tr>
            <td>Stranger Things: Stranger Things 3: Chapter Five: The Flayed (Episode 5)</td>
            <td>9</td>
        </tr>
        <tr>
            <td>Stranger Things: Stranger Things 3: Chapter Four: The Sauna Test (Episode 4)</td>
            <td>9</td>
        </tr>
        <tr>
            <td>Stranger Things: Stranger Things 3: Chapter One: Suzie, Do You Copy? (Episode 1)</td>
            <td>9</td>
        </tr>
    </tbody>
</table>



There are 9 distinct titles left but after a quick look at the result of these lines, we see that there are only 2 different series: Roman Empire and Stranger Things.

So we can manually define that part 2 is the season for Stranger Things. And parts 2 and 3 combined are the season for Roman Empire. (We can't guess like that, I had to look it up on Netflix). 

We'll put 2 for the season part of Roman Empire and adjust it manually later. (I have to be careful not to forget!)

Let's check if the selected part is only the good one (the season one) and that we have no errors:

1) For the 4 part titles with the word 'Season':


```sql
%%sql 

SELECT title_part, COUNT(*) AS total_distinct_titles
FROM v3
WHERE total_parts > 3
AND title_part LIKE '%Season%'
GROUP BY title_part
ORDER BY title_part;
```

     * postgresql://postgres:***@localhost:5433/N1
    7 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title_part</th>
            <th>total_distinct_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td> Season 1</td>
            <td>50</td>
        </tr>
        <tr>
            <td> Season 2</td>
            <td>28</td>
        </tr>
        <tr>
            <td> Season 3</td>
            <td>20</td>
        </tr>
        <tr>
            <td> Season 4</td>
            <td>11</td>
        </tr>
        <tr>
            <td> Season 5</td>
            <td>1</td>
        </tr>
        <tr>
            <td> Season 8</td>
            <td>2</td>
        </tr>
        <tr>
            <td> Season 9</td>
            <td>3</td>
        </tr>
    </tbody>
</table>



Ok no problems.

2) For the 4 part titles with the word 'Serie':


```sql
%%sql 

SELECT title_part, COUNT(*) AS total_distinct_titles
FROM v3
WHERE total_parts > 3
AND title_part LIKE '%Serie%'
GROUP BY title_part
ORDER BY title_part;
```

     * postgresql://postgres:***@localhost:5433/N1
    2 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title_part</th>
            <th>total_distinct_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td> Limited Series</td>
            <td>13</td>
        </tr>
        <tr>
            <td>A Series of Unfortunate Events</td>
            <td>1</td>
        </tr>
    </tbody>
</table>



Problem: "A Series of Unfortunate Events" is the title of the series. Not the season part. It shouldn't be here.

Normally, there are only 3 parts for this serie and the second part is the season part, but for an episode, there is a 4th part. Futhermore, in the title there is the word "Serie", see below. (that's "Unfortunate" for us ;) ).


```sql
%%sql 

SELECT DISTINCT title
FROM netflix_tt
WHERE title LIKE '%A Series of Unfortunate Events%';
```

     * postgresql://postgres:***@localhost:5433/N1
    2 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>A Series of Unfortunate Events: Season 3: Penultimate Peril: Part 2 (Episode 6)</td>
        </tr>
        <tr>
            <td>A Series of Unfortunate Events: Season 3: The End (Episode 7)</td>
        </tr>
    </tbody>
</table>



We can see that Serie and Season are present in this title, so the following change should fix the problem:


```sql
%%sql 

SELECT title_part, COUNT(*) AS total_distinct_titles
FROM v3
WHERE total_parts > 3
AND title_part LIKE '%Serie%'
AND title_part NOT LIKE '%Unfortunate%'
GROUP BY title_part
ORDER BY title_part;
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title_part</th>
            <th>total_distinct_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td> Limited Series</td>
            <td>13</td>
        </tr>
    </tbody>
</table>



Ok good. 

Now, let's use COALESCE and MIN()OVER(PARTITION BY num_title)
to create the column season_part:

(I'll put also: if season_part = 999 that means we have an error.)


```sql
%%sql 

CREATE VIEW v4 AS

SELECT *, 
    COALESCE(MIN(CASE
                WHEN total_parts = 3 THEN 2
                WHEN total_parts > 3 AND title_part LIKE '%Season%' THEN num_part
                WHEN total_parts > 3 AND title_part LIKE '%Serie%'
                 AND title_part NOT LIKE '%Unfortunate%' THEN num_part
                WHEN title LIKE '%Stranger Things%' THEN 2
                WHEN title LIKE '%Roman Empire%' THEN 2
                ELSE 999 END) OVER(PARTITION BY num_title), 1) AS season_part
FROM v3
ORDER BY num_title, num_part;

SELECT *
FROM v4
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>num_title</th>
            <th>title</th>
            <th>title_part</th>
            <th>num_part</th>
            <th>total_parts</th>
            <th>season_part</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td>3%</td>
            <td>1</td>
            <td>4</td>
            <td>2</td>
        </tr>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td> Season 1</td>
            <td>2</td>
            <td>4</td>
            <td>2</td>
        </tr>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td> Chapter 01</td>
            <td>3</td>
            <td>4</td>
            <td>2</td>
        </tr>
        <tr>
            <td>1</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
            <td> Cubes (Episode 1)</td>
            <td>4</td>
            <td>4</td>
            <td>2</td>
        </tr>
        <tr>
            <td>2</td>
            <td>3%: Season 1: Chapter 02: Coins (Episode 2)</td>
            <td>3%</td>
            <td>1</td>
            <td>4</td>
            <td>2</td>
        </tr>
    </tbody>
</table>



Let's check:


```sql
%%sql 

SELECT season_part, COUNT(*) AS row_num
FROM v4
GROUP BY season_part
ORDER BY row_num DESC;
```

     * postgresql://postgres:***@localhost:5433/N1
    2 rows affected.
    




<table>
    <thead>
        <tr>
            <th>season_part</th>
            <th>row_num</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2</td>
            <td>3347</td>
        </tr>
        <tr>
            <td>3</td>
            <td>264</td>
        </tr>
    </tbody>
</table>



Ok we don’t have '999' (which means error), only 2 and 3, that seems good!

For season_part = 2 :


```sql
%%sql 

SELECT title, season_part
FROM v4
WHERE season_part = 2
ORDER BY RANDOM()
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
            <th>season_part</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>3%: Season 2: Chapter 07: Fog (Episode 7)</td>
            <td>2</td>
        </tr>
        <tr>
            <td>3%: Season 2: Chapter 09: Necklace (Episode 9)</td>
            <td>2</td>
        </tr>
        <tr>
            <td>Hero Corp: Season 2: Une nouvelle ère: partie 2 (Episode 15)</td>
            <td>2</td>
        </tr>
        <tr>
            <td>3%: Season 1: Chapter 02: Coins (Episode 2)</td>
            <td>2</td>
        </tr>
        <tr>
            <td>H: Season 3: The Noose (Episode 5)</td>
            <td>2</td>
        </tr>
    </tbody>
</table>



All seems good.

For season_part = 3 :


```sql
%%sql 

SELECT title, season_part
FROM v4
WHERE season_part = 3
ORDER BY RANDOM()
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
            <th>season_part</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Formula 1: Drive to Survive: Season 1: Keeping Your Head (Episode 7)</td>
            <td>3</td>
        </tr>
        <tr>
            <td>Formula 1: Drive to Survive: Season 2: Lights Out (Episode 1)</td>
            <td>3</td>
        </tr>
        <tr>
            <td>Formula 1: Drive to Survive: Season 2: Lights Out (Episode 1)</td>
            <td>3</td>
        </tr>
        <tr>
            <td>Formula 1: Drive to Survive: Season 2: Boiling Point (Episode 2)</td>
            <td>3</td>
        </tr>
        <tr>
            <td>Last Chance U: Basketball: Season 1: The Window (Episode 1)</td>
            <td>3</td>
        </tr>
    </tbody>
</table>



All seems good too.


##### 4.4) Gathering all the parts of the title/season/episode with ARRAY_AGG() and ARRAY_TO_STRING(). <a class="anchor" id="section_2_3_4_4"></a>

Now let's go from array to 3 strings: title, season and episode by using ARRAY_AGG() to gather all the parts of the title/season/episode. And then with ARRAY_TO_STRING() to create a string with these parts:


```sql
%%sql 

CREATE VIEW v5 AS

SELECT num_title,
    ARRAY_TO_STRING(ARRAY_AGG(CASE WHEN num_part < season_part THEN title_part END), ': ') AS new_title,
    ARRAY_TO_STRING(ARRAY_AGG(CASE WHEN num_part = season_part THEN title_part END), ': ') AS season,
    ARRAY_TO_STRING(ARRAY_AGG(CASE WHEN num_part > season_part THEN title_part END), ': ') AS episode,
    title AS old_title
FROM v4
GROUP BY num_title, old_title;

SELECT *
FROM v5
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>num_title</th>
            <th>new_title</th>
            <th>season</th>
            <th>episode</th>
            <th>old_title</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1</td>
            <td>3%</td>
            <td> Season 1</td>
            <td> Chapter 01:  Cubes (Episode 1)</td>
            <td>3%: Season 1: Chapter 01: Cubes (Episode 1)</td>
        </tr>
        <tr>
            <td>2</td>
            <td>3%</td>
            <td> Season 1</td>
            <td> Chapter 02:  Coins (Episode 2)</td>
            <td>3%: Season 1: Chapter 02: Coins (Episode 2)</td>
        </tr>
        <tr>
            <td>3</td>
            <td>3%</td>
            <td> Season 1</td>
            <td> Chapter 03:  Corridor (Episode 3)</td>
            <td>3%: Season 1: Chapter 03: Corridor (Episode 3)</td>
        </tr>
        <tr>
            <td>4</td>
            <td>3%</td>
            <td> Season 1</td>
            <td> Chapter 04:  Gateway (Episode 4)</td>
            <td>3%: Season 1: Chapter 04: Gateway (Episode 4)</td>
        </tr>
        <tr>
            <td>5</td>
            <td>3%</td>
            <td> Season 1</td>
            <td> Chapter 05:  Water (Episode 5)</td>
            <td>3%: Season 1: Chapter 05: Water (Episode 5)</td>
        </tr>
    </tbody>
</table>



Yeah ! It seems to work !

Let’s just do the last check:


```sql
%%sql 

SELECT COUNT(DISTINCT title)
FROM netflix_tt
WHERE type = 'Serie';
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1158</td>
        </tr>
    </tbody>
</table>




```sql
%%sql 

SELECT COUNT(*)
FROM v5;
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1158</td>
        </tr>
    </tbody>
</table>



Ok good.

##### 4.5) Inserting the new series/season/episode titles into our original table. <a class="anchor" id="section_2_3_4_5"></a>
Now, let’s join netflix_tt and the new serie title format of v5 and save the result into a new table: netflix_tt_final


```sql
%%sql 

SELECT n.*, v5.new_title, v5.season, v5.episode
INTO TABLE netflix_tt_final
FROM netflix_tt n
LEFT JOIN v5
ON n.title = v5.old_title
ORDER BY datetime DESC;

SELECT *
FROM netflix_tt_final
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    1867 rows affected.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>profile</th>
            <th>datetime</th>
            <th>duration</th>
            <th>title</th>
            <th>device</th>
            <th>country</th>
            <th>type</th>
            <th>new_title</th>
            <th>season</th>
            <th>episode</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>TT</td>
            <td>2021-07-13 14:53:45</td>
            <td>01:34:18</td>
            <td>How I Became a Superhero</td>
            <td>Computer</td>
            <td>France</td>
            <td>Movie</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 10:15:31</td>
            <td>00:35:57</td>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> Big Man (Episode 8)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 08:13:56</td>
            <td>00:10:34</td>
            <td>Sweet Tooth: Season 1: Big Man (Episode 8)</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> Big Man (Episode 8)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 07:37:58</td>
            <td>00:35:57</td>
            <td>Sweet Tooth: Season 1: When Pubba Met Birdie (Episode 7)</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> When Pubba Met Birdie (Episode 7)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 07:26:29</td>
            <td>00:11:28</td>
            <td>Sweet Tooth: Season 1: Stranger Danger on a Train (Episode 6)</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> Stranger Danger on a Train (Episode 6)</td>
        </tr>
    </tbody>
</table>



Ok, now we need to put the title of the movies in the new_title column. And by the way, I'm going to change the names of these two columns to old_title and title and finally drop the old_title column. 


```sql
%%sql 

ALTER TABLE netflix_tt_final
RENAME COLUMN title TO old_title; 

ALTER TABLE netflix_tt_final
RENAME COLUMN new_title TO title; 

UPDATE netflix_tt_final
SET title = old_title
WHERE title IS NULL;

ALTER TABLE netflix_tt_final
DROP COLUMN old_title; 

SELECT *
FROM netflix_tt_final
ORDER BY datetime DESC
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    Done.
    237 rows affected.
    Done.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>profile</th>
            <th>datetime</th>
            <th>duration</th>
            <th>device</th>
            <th>country</th>
            <th>type</th>
            <th>title</th>
            <th>season</th>
            <th>episode</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>TT</td>
            <td>2021-07-13 14:53:45</td>
            <td>01:34:18</td>
            <td>Computer</td>
            <td>France</td>
            <td>Movie</td>
            <td>How I Became a Superhero</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 10:15:31</td>
            <td>00:35:57</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> Big Man (Episode 8)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 08:13:56</td>
            <td>00:10:34</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> Big Man (Episode 8)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 07:37:58</td>
            <td>00:35:57</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> When Pubba Met Birdie (Episode 7)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 07:26:29</td>
            <td>00:11:28</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> Stranger Danger on a Train (Episode 6)</td>
        </tr>
    </tbody>
</table>



Yeah now we’re good !

Hop hop hop, it's not finished! We haven't forgotten about Roman Empire, let's correct that:


```sql
%%sql 

SELECT title, season, episode
FROM netflix_tt_final
WHERE title LIKE '%Roman Empire%';
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
            <th>season</th>
            <th>episode</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Roman Empire</td>
            <td> Commodus</td>
            <td> Reign of Blood:  Born in the Purple (Episode 1)</td>
        </tr>
    </tbody>
</table>




```sql
%%sql 

UPDATE netflix_tt_final
SET season = CONCAT(season, ': ', SUBSTRING(episode, 0, POSITION(':' IN episode)))
WHERE title LIKE '%Roman Empire%';

UPDATE netflix_tt_final
SET episode = SUBSTRING(episode, POSITION(':' IN episode) + 2, LENGTH(episode))
WHERE title LIKE '%Roman Empire%';

SELECT title, season, episode
FROM netflix_tt_final
WHERE title LIKE '%Roman Empire%';
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    1 rows affected.
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>title</th>
            <th>season</th>
            <th>episode</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Roman Empire</td>
            <td> Commodus:  Reign of Blood</td>
            <td> Born in the Purple (Episode 1)</td>
        </tr>
    </tbody>
</table>



OK so now we are good for the titles!


```sql
%%sql 

SELECT *
FROM netflix_tt_final
ORDER BY datetime DESC
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>profile</th>
            <th>datetime</th>
            <th>duration</th>
            <th>device</th>
            <th>country</th>
            <th>type</th>
            <th>title</th>
            <th>season</th>
            <th>episode</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>TT</td>
            <td>2021-07-13 14:53:45</td>
            <td>01:34:18</td>
            <td>Computer</td>
            <td>France</td>
            <td>Movie</td>
            <td>How I Became a Superhero</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 10:15:31</td>
            <td>00:35:57</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> Big Man (Episode 8)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 08:13:56</td>
            <td>00:10:34</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> Big Man (Episode 8)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 07:37:58</td>
            <td>00:35:57</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> When Pubba Met Birdie (Episode 7)</td>
        </tr>
        <tr>
            <td>TT</td>
            <td>2021-07-02 07:26:29</td>
            <td>00:11:28</td>
            <td>Phone</td>
            <td>France</td>
            <td>Serie</td>
            <td>Sweet Tooth</td>
            <td> Season 1</td>
            <td> Stranger Danger on a Train (Episode 6)</td>
        </tr>
    </tbody>
</table>



#### 4 - Generating a serie of dates <a class="anchor" id="section_2_4"></a>

To better analyze when I watched Netflix and when I didn't, I need to add a line for each day of the period, even if I didn't watch Netflix that day (=row null with duration = '00:00:00').

For that, we need to find the first and last date of the period:


```sql
%%sql 

SELECT MIN(datetime), MAX(datetime)
FROM netflix_tt_final;
```

     * postgresql://postgres:***@localhost:5433/N1
    1 rows affected.
    




<table>
    <thead>
        <tr>
            <th>min</th>
            <th>max</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2019-01-14 21:32:57</td>
            <td>2021-07-13 14:53:45</td>
        </tr>
    </tbody>
</table>



Then generate a serie of dates:


```sql
%%sql 

SELECT date_trunc('day', dd)::date AS datelist
FROM generate_series('2019-01-14'::date, '2021-07-13'::date, '1 day'::interval) AS dd
ORDER BY datelist DESC
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>datelist</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2021-07-13</td>
        </tr>
        <tr>
            <td>2021-07-12</td>
        </tr>
        <tr>
            <td>2021-07-11</td>
        </tr>
        <tr>
            <td>2021-07-10</td>
        </tr>
        <tr>
            <td>2021-07-09</td>
        </tr>
    </tbody>
</table>



Then join this datelist with netflix_tt_final and save all that into a new table netflix_tt_fullperiod:


```sql
%%sql 

WITH a AS (SELECT date_trunc('day', dd)::date AS datelist
    FROM generate_series('2019-01-14'::date, '2021-07-13'::date, '1 day'::interval) AS dd
    ORDER BY datelist DESC)

SELECT a.datelist, n.*
INTO TABLE netflix_tt_fullperiod
FROM a
LEFT JOIN netflix_tt_final n
ON a.datelist = DATE(n.datetime)
ORDER BY a.datelist DESC;

SELECT *
FROM netflix_tt_fullperiod
ORDER BY datelist DESC
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    2348 rows affected.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>datelist</th>
            <th>profile</th>
            <th>datetime</th>
            <th>duration</th>
            <th>device</th>
            <th>country</th>
            <th>type</th>
            <th>title</th>
            <th>season</th>
            <th>episode</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2021-07-13</td>
            <td>TT</td>
            <td>2021-07-13 14:53:45</td>
            <td>01:34:18</td>
            <td>Computer</td>
            <td>France</td>
            <td>Movie</td>
            <td>How I Became a Superhero</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2021-07-12</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2021-07-11</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2021-07-10</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2021-07-09</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
    </tbody>
</table>



We can see that some rows are null because I didn't watch Netflix on those days. Now let's set the duration = '00:00:00' when it's null.


```sql
%%sql 

UPDATE netflix_tt_fullperiod
SET duration = '00:00:00'
WHERE duration IS NULL;

SELECT *
FROM netflix_tt_fullperiod
ORDER BY datelist DESC
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    481 rows affected.
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>datelist</th>
            <th>profile</th>
            <th>datetime</th>
            <th>duration</th>
            <th>device</th>
            <th>country</th>
            <th>type</th>
            <th>title</th>
            <th>season</th>
            <th>episode</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2021-07-13</td>
            <td>TT</td>
            <td>2021-07-13 14:53:45</td>
            <td>01:34:18</td>
            <td>Computer</td>
            <td>France</td>
            <td>Movie</td>
            <td>How I Became a Superhero</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2021-07-12</td>
            <td>None</td>
            <td>None</td>
            <td>00:00:00</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2021-07-11</td>
            <td>None</td>
            <td>None</td>
            <td>00:00:00</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2021-07-10</td>
            <td>None</td>
            <td>None</td>
            <td>00:00:00</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>2021-07-09</td>
            <td>None</td>
            <td>None</td>
            <td>00:00:00</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
    </tbody>
</table>



OK! Now we are good for the analysis.

## B) Analysis <a class="anchor" id="chapter3"></a>

The objectives are :

   - To know my top 5 series and my top 5 movies.
   - To compare my consumption of series vs movies (overall consumption but also by year).
   - To know the months when I consume Netflix the most.
   - To analyse on which device I watch Netflix in general but also by hour of the day.
   - To analyse where in the world I watched Netflix from 2019 to 2021.

### 1- Top 5 Series <a class="anchor" id="section_3_1"></a>


```sql
%%sql 

SELECT title AS serie_title, 
    ROUND( CAST(SUM(EXTRACT(epoch FROM duration)/3600) as numeric), 1) AS total_hours
FROM netflix_tt_fullperiod
WHERE type = 'Serie'
GROUP BY title
ORDER BY total_hours DESC
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>serie_title</th>
            <th>total_hours</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>How I Met Your Mother</td>
            <td>74.2</td>
        </tr>
        <tr>
            <td>Peaky Blinders</td>
            <td>58.8</td>
        </tr>
        <tr>
            <td>Breaking Bad</td>
            <td>41.3</td>
        </tr>
        <tr>
            <td>The Crown</td>
            <td>36.2</td>
        </tr>
        <tr>
            <td>H</td>
            <td>31.4</td>
        </tr>
    </tbody>
</table>



### 2 - Top 5 Movies <a class="anchor" id="section_3_2"></a>


```sql
%%sql 

SELECT title AS serie_title,
    ROUND( CAST(SUM(EXTRACT(epoch FROM duration)/3600) as numeric), 1) AS total_hours
FROM netflix_tt_fullperiod
WHERE type = 'Movie'
GROUP BY title
ORDER BY total_hours DESC
LIMIT 5;
```

     * postgresql://postgres:***@localhost:5433/N1
    5 rows affected.
    




<table>
    <thead>
        <tr>
            <th>serie_title</th>
            <th>total_hours</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>The Big Short</td>
            <td>4.3</td>
        </tr>
        <tr>
            <td>I Care a Lot</td>
            <td>3.8</td>
        </tr>
        <tr>
            <td>War Dogs</td>
            <td>3.6</td>
        </tr>
        <tr>
            <td>Spirited Away</td>
            <td>3.2</td>
        </tr>
        <tr>
            <td>The Dark Knight</td>
            <td>3.1</td>
        </tr>
    </tbody>
</table>



### 3 - Series VS Movies <a class="anchor" id="section_3_3"></a>


```sql
%%sql 

WITH a AS (SELECT type,
                ROUND( CAST(SUM(EXTRACT(epoch FROM duration)/3600) as numeric)) AS total_hours
            FROM netflix_tt_fullperiod
            WHERE type IS NOT NULL
            GROUP BY type
            ORDER BY total_hours DESC)

SELECT *, 
    ROUND(total_hours / SUM(total_hours) OVER ()*100) AS "% of total"
FROM a;
```

     * postgresql://postgres:***@localhost:5433/N1
    2 rows affected.
    




<table>
    <thead>
        <tr>
            <th>type</th>
            <th>total_hours</th>
            <th>% of total</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Serie</td>
            <td>765</td>
            <td>74</td>
        </tr>
        <tr>
            <td>Movie</td>
            <td>271</td>
            <td>26</td>
        </tr>
    </tbody>
</table>



Interpretation: I watched 3 times more series than films (in hours).


```sql
%%sql 

SELECT CAST(EXTRACT(YEAR FROM datelist) as integer) AS year,
        COUNT(DISTINCT episode) FILTER(WHERE type = 'Serie') AS nb_of_serie_episodes,
        COUNT(DISTINCT title) FILTER (WHERE type = 'Movie') AS nb_of_movies
FROM netflix_tt_fullperiod
GROUP BY year
ORDER BY year DESC;

```

     * postgresql://postgres:***@localhost:5433/N1
    3 rows affected.
    




<table>
    <thead>
        <tr>
            <th>year</th>
            <th>nb_of_serie_episodes</th>
            <th>nb_of_movies</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2021</td>
            <td>211</td>
            <td>30</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>427</td>
            <td>52</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>492</td>
            <td>78</td>
        </tr>
    </tbody>
</table>



Interpretation: 

Between 2019 and 2020: I seem to have watched a bit less series (-13%) in 2020 than in 2019 and less films (-33%) in 2020 than in 2019.
The end of the analysis period is July 2021, so no conclusion can be drawn about 2021 for the moment (but I kept it to get a general idea after 7 months). 

### 4 - My Netflix consumption per month <a class="anchor" id="section_3_4"></a>


```sql
%%sql 

CREATE VIEW vmonth AS

SELECT CAST(EXTRACT(YEAR FROM datelist) as integer) AS year,
        CAST( EXTRACT(MONTH FROM datelist) as integer) AS month_num,
        TO_CHAR(datelist, 'month') AS month,
        COUNT(title) AS total_titles_watched
FROM netflix_tt_fullperiod
WHERE  EXTRACT(YEAR FROM datelist) != 2021 OR  EXTRACT(MONTH FROM datelist) != 7
GROUP BY year, month_num, month
ORDER BY year DESC, month_num DESC;

SELECT *
FROM vmonth;
```

     * postgresql://postgres:***@localhost:5433/N1
    Done.
    30 rows affected.
    




<table>
    <thead>
        <tr>
            <th>year</th>
            <th>month_num</th>
            <th>month</th>
            <th>total_titles_watched</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2021</td>
            <td>6</td>
            <td>june     </td>
            <td>61</td>
        </tr>
        <tr>
            <td>2021</td>
            <td>5</td>
            <td>may      </td>
            <td>86</td>
        </tr>
        <tr>
            <td>2021</td>
            <td>4</td>
            <td>april    </td>
            <td>16</td>
        </tr>
        <tr>
            <td>2021</td>
            <td>3</td>
            <td>march    </td>
            <td>68</td>
        </tr>
        <tr>
            <td>2021</td>
            <td>2</td>
            <td>february </td>
            <td>64</td>
        </tr>
        <tr>
            <td>2021</td>
            <td>1</td>
            <td>january  </td>
            <td>51</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>12</td>
            <td>december </td>
            <td>67</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>11</td>
            <td>november </td>
            <td>42</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>10</td>
            <td>october  </td>
            <td>96</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>9</td>
            <td>september</td>
            <td>33</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>8</td>
            <td>august   </td>
            <td>64</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>7</td>
            <td>july     </td>
            <td>116</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>6</td>
            <td>june     </td>
            <td>55</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>5</td>
            <td>may      </td>
            <td>14</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>4</td>
            <td>april    </td>
            <td>0</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>3</td>
            <td>march    </td>
            <td>6</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>2</td>
            <td>february </td>
            <td>135</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>1</td>
            <td>january  </td>
            <td>26</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>12</td>
            <td>december </td>
            <td>13</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>11</td>
            <td>november </td>
            <td>40</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>10</td>
            <td>october  </td>
            <td>71</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>9</td>
            <td>september</td>
            <td>63</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>8</td>
            <td>august   </td>
            <td>143</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>7</td>
            <td>july     </td>
            <td>113</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>6</td>
            <td>june     </td>
            <td>75</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>5</td>
            <td>may      </td>
            <td>47</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>4</td>
            <td>april    </td>
            <td>51</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>3</td>
            <td>march    </td>
            <td>173</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>2</td>
            <td>february </td>
            <td>56</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>1</td>
            <td>january  </td>
            <td>12</td>
        </tr>
    </tbody>
</table>




```sql
%%sql 

SELECT month_num, month,
    ROUND(AVG(total_titles_watched)) AS avg_titles_watched
FROM vmonth
GROUP BY month_num, month
ORDER BY month_num DESC;
```

     * postgresql://postgres:***@localhost:5433/N1
    12 rows affected.
    




<table>
    <thead>
        <tr>
            <th>month_num</th>
            <th>month</th>
            <th>avg_titles_watched</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>12</td>
            <td>december </td>
            <td>40</td>
        </tr>
        <tr>
            <td>11</td>
            <td>november </td>
            <td>41</td>
        </tr>
        <tr>
            <td>10</td>
            <td>october  </td>
            <td>84</td>
        </tr>
        <tr>
            <td>9</td>
            <td>september</td>
            <td>48</td>
        </tr>
        <tr>
            <td>8</td>
            <td>august   </td>
            <td>104</td>
        </tr>
        <tr>
            <td>7</td>
            <td>july     </td>
            <td>115</td>
        </tr>
        <tr>
            <td>6</td>
            <td>june     </td>
            <td>64</td>
        </tr>
        <tr>
            <td>5</td>
            <td>may      </td>
            <td>49</td>
        </tr>
        <tr>
            <td>4</td>
            <td>april    </td>
            <td>22</td>
        </tr>
        <tr>
            <td>3</td>
            <td>march    </td>
            <td>82</td>
        </tr>
        <tr>
            <td>2</td>
            <td>february </td>
            <td>85</td>
        </tr>
        <tr>
            <td>1</td>
            <td>january  </td>
            <td>30</td>
        </tr>
    </tbody>
</table>



Comment: 
 I had to remove July 2021 as we do not have the full month's data. This would therefore create an error in the average.

We only have data for 2019, 2020 and half of 2021. It is therefore complicated to draw real conclusions on the monthly average. (sampling error). That's why the average after July is only calculated with 2019 and 2020.

Interpretation: It seems that I watch the most Netflix during the summer: June, July and August. But also in February, March and October.

### 5 - Device analysis: On what do I watch Netflix? <a class="anchor" id="section_3_5"></a>


```sql
%%sql 

SELECT device,
    COUNT(title) AS nb_of_titles
FROM netflix_tt_fullperiod
WHERE device IS NOT NULL
GROUP BY device
ORDER BY nb_of_titles DESC;
```

     * postgresql://postgres:***@localhost:5433/N1
    3 rows affected.
    




<table>
    <thead>
        <tr>
            <th>device</th>
            <th>nb_of_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Computer</td>
            <td>1524</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>280</td>
        </tr>
        <tr>
            <td>PS4</td>
            <td>63</td>
        </tr>
    </tbody>
</table>



Interpretation:

I most often use my computer to watch Netflix, then my phone and sometimes my PS4.

Let's see the number of titles watched by hours of the day:


```sql
%%sql 

SELECT device,
    CASE WHEN EXTRACT(HOUR FROM datetime) < 4 THEN '0-4H'
    WHEN EXTRACT(HOUR FROM datetime) >= 4 AND EXTRACT(HOUR FROM datetime) < 6 THEN '4-6H'
    WHEN EXTRACT(HOUR FROM datetime) >= 6 AND EXTRACT(HOUR FROM datetime) < 8 THEN '6-8H'
    WHEN EXTRACT(HOUR FROM datetime) >= 8 AND EXTRACT(HOUR FROM datetime) < 10 THEN '8-10H'
    WHEN EXTRACT(HOUR FROM datetime) >= 10 AND EXTRACT(HOUR FROM datetime) < 12 THEN '10-12H'
    WHEN EXTRACT(HOUR FROM datetime) >= 12 AND EXTRACT(HOUR FROM datetime) < 14 THEN '12-14H'
    WHEN EXTRACT(HOUR FROM datetime) >= 14 AND EXTRACT(HOUR FROM datetime) < 16 THEN '14-16H'
    WHEN EXTRACT(HOUR FROM datetime) >= 16 AND EXTRACT(HOUR FROM datetime) < 18 THEN '16-18H'
    WHEN EXTRACT(HOUR FROM datetime) >= 18 AND EXTRACT(HOUR FROM datetime) < 20 THEN '18-20H'
    WHEN EXTRACT(HOUR FROM datetime) >= 20 AND EXTRACT(HOUR FROM datetime) < 22 THEN '20-22H'
    WHEN EXTRACT(HOUR FROM datetime) >= 22 AND EXTRACT(HOUR FROM datetime) < 24 THEN '22-24H'
    ELSE 'error?' END AS hour_of_day,
    COUNT(title) AS nb_of_titles
FROM netflix_tt_fullperiod
WHERE device IS NOT NULL
GROUP BY device, hour_of_day
ORDER BY device, nb_of_titles DESC;



```

     * postgresql://postgres:***@localhost:5433/N1
    29 rows affected.
    




<table>
    <thead>
        <tr>
            <th>device</th>
            <th>hour_of_day</th>
            <th>nb_of_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Computer</td>
            <td>20-22H</td>
            <td>291</td>
        </tr>
        <tr>
            <td>Computer</td>
            <td>22-24H</td>
            <td>241</td>
        </tr>
        <tr>
            <td>Computer</td>
            <td>18-20H</td>
            <td>222</td>
        </tr>
        <tr>
            <td>Computer</td>
            <td>10-12H</td>
            <td>165</td>
        </tr>
        <tr>
            <td>Computer</td>
            <td>16-18H</td>
            <td>143</td>
        </tr>
        <tr>
            <td>Computer</td>
            <td>12-14H</td>
            <td>141</td>
        </tr>
        <tr>
            <td>Computer</td>
            <td>14-16H</td>
            <td>111</td>
        </tr>
        <tr>
            <td>Computer</td>
            <td>0-4H</td>
            <td>101</td>
        </tr>
        <tr>
            <td>Computer</td>
            <td>8-10H</td>
            <td>83</td>
        </tr>
        <tr>
            <td>Computer</td>
            <td>6-8H</td>
            <td>26</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>6-8H</td>
            <td>107</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>16-18H</td>
            <td>50</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>22-24H</td>
            <td>31</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>20-22H</td>
            <td>25</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>18-20H</td>
            <td>16</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>10-12H</td>
            <td>16</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>8-10H</td>
            <td>14</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>0-4H</td>
            <td>10</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>14-16H</td>
            <td>6</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>4-6H</td>
            <td>3</td>
        </tr>
        <tr>
            <td>Phone</td>
            <td>12-14H</td>
            <td>2</td>
        </tr>
        <tr>
            <td>PS4</td>
            <td>22-24H</td>
            <td>15</td>
        </tr>
        <tr>
            <td>PS4</td>
            <td>20-22H</td>
            <td>14</td>
        </tr>
        <tr>
            <td>PS4</td>
            <td>18-20H</td>
            <td>9</td>
        </tr>
        <tr>
            <td>PS4</td>
            <td>0-4H</td>
            <td>8</td>
        </tr>
        <tr>
            <td>PS4</td>
            <td>16-18H</td>
            <td>6</td>
        </tr>
        <tr>
            <td>PS4</td>
            <td>12-14H</td>
            <td>5</td>
        </tr>
        <tr>
            <td>PS4</td>
            <td>14-16H</td>
            <td>4</td>
        </tr>
        <tr>
            <td>PS4</td>
            <td>10-12H</td>
            <td>2</td>
        </tr>
    </tbody>
</table>



Interpretation:

We can see that I watch Netflix with my computer and my PS4 mostly in the evening. But for my phone, I use it mostly during my commute to work or when I'm travelling (train, bus, metro, ...): between 6-8H or 16-18H.

### 6 - Country analysis: Where in the world did I watch Netflix? <a class="anchor" id="section_3_6"></a>


```sql
%%sql 

SELECT CAST(EXTRACT(YEAR FROM datetime) as integer) AS year,
    MIN(DATE(datetime)) AS start_date,
    MAX(DATE(datetime)) AS end_date,
    country,
    COUNT(title) AS nb_of_titles
FROM netflix_tt_fullperiod
WHERE country IS NOT NULL
GROUP BY year, country
ORDER BY year DESC, start_date DESC;
```

     * postgresql://postgres:***@localhost:5433/N1
    8 rows affected.
    




<table>
    <thead>
        <tr>
            <th>year</th>
            <th>start_date</th>
            <th>end_date</th>
            <th>country</th>
            <th>nb_of_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2021</td>
            <td>2021-01-04</td>
            <td>2021-07-13</td>
            <td>France</td>
            <td>356</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>2020-05-08</td>
            <td>2020-12-30</td>
            <td>France</td>
            <td>487</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>2020-01-05</td>
            <td>2020-03-19</td>
            <td>United Kingdom</td>
            <td>161</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>2020-01-04</td>
            <td>2020-01-04</td>
            <td>China</td>
            <td>2</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>2020-01-03</td>
            <td>2020-01-03</td>
            <td>Cambodia</td>
            <td>4</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>2019-12-23</td>
            <td>2019-12-24</td>
            <td>Cambodia</td>
            <td>4</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>2019-09-25</td>
            <td>2019-12-13</td>
            <td>United Kingdom</td>
            <td>137</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>2019-01-14</td>
            <td>2019-09-19</td>
            <td>France</td>
            <td>716</td>
        </tr>
    </tbody>
</table>



Adding some comments:


```sql
%%sql 

SELECT CAST(EXTRACT(YEAR FROM datetime) as integer) AS year,
    MIN(DATE(datetime)) AS start_date,
    MAX(DATE(datetime)) AS end_date,
    CASE WHEN MAX(DATE(datetime)) = '2019-09-19' THEN 'Leaving FR to start my second MSc in UK'
    WHEN MAX(DATE(datetime)) = '2019-12-13' THEN 'Going on vacation in Cambodia'
    WHEN MAX(DATE(datetime)) = '2020-01-04' THEN 'Transit through China to return to UK'
    WHEN MAX(DATE(datetime)) = '2020-03-19' THEN 'COVID: Leaving UK for France'
    WHEN MAX(DATE(datetime)) = '2021-07-13' THEN 'Last date recorded'
    ELSE '--' END AS end_date_comment,
    country,
    COUNT(title) AS nb_of_titles
FROM netflix_tt_fullperiod
WHERE country IS NOT NULL
GROUP BY year, country
ORDER BY year DESC, start_date DESC;
```

     * postgresql://postgres:***@localhost:5433/N1
    8 rows affected.
    




<table>
    <thead>
        <tr>
            <th>year</th>
            <th>start_date</th>
            <th>end_date</th>
            <th>end_date_comment</th>
            <th>country</th>
            <th>nb_of_titles</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2021</td>
            <td>2021-01-04</td>
            <td>2021-07-13</td>
            <td>Last date recorded</td>
            <td>France</td>
            <td>356</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>2020-05-08</td>
            <td>2020-12-30</td>
            <td>--</td>
            <td>France</td>
            <td>487</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>2020-01-05</td>
            <td>2020-03-19</td>
            <td>COVID: Leaving UK for France</td>
            <td>United Kingdom</td>
            <td>161</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>2020-01-04</td>
            <td>2020-01-04</td>
            <td>Transit through China to return to UK</td>
            <td>China</td>
            <td>2</td>
        </tr>
        <tr>
            <td>2020</td>
            <td>2020-01-03</td>
            <td>2020-01-03</td>
            <td>--</td>
            <td>Cambodia</td>
            <td>4</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>2019-12-23</td>
            <td>2019-12-24</td>
            <td>--</td>
            <td>Cambodia</td>
            <td>4</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>2019-09-25</td>
            <td>2019-12-13</td>
            <td>Going on vacation in Cambodia</td>
            <td>United Kingdom</td>
            <td>137</td>
        </tr>
        <tr>
            <td>2019</td>
            <td>2019-01-14</td>
            <td>2019-09-19</td>
            <td>Leaving FR to start my second MSc in UK</td>
            <td>France</td>
            <td>716</td>
        </tr>
    </tbody>
</table>



Interpretation:

We can see that I have travelled quite a lot in 2019-2020: France, UK, Cambodia, China (this was just before COVID). And when I travel, I read books and sometimes I watch a little Netflix :) . We can see that with Cambodia and China.

Besides that, I've lived in France and the UK, that is why we can see that those are the places where I watched the most Netflix.

### C) Visualisation with Tableau <a class="anchor" id="chapter4"></a>

I wanted to do this project with 100% SQL to practice my SQL skills. Indeed, we can see all the results of the analysis only with SQL but I will still add (just below) a visual of these results to make them more pleasant to read and understand. I did it with Tableau:


```python
from IPython.display import Image
Image(r'C:\Users\Tristan\Documents\DATA\netflix project\Netflix_tt 1.png')
```




    
![png](output_130_0.png)
    




```python
Image(r'C:\Users\Tristan\Documents\DATA\netflix project\Netflix_tt 2.png')
```




    
![png](output_131_0.png)
    



### Conclusion <a class="anchor" id="chapter5"></a>


So, to recap: I watch a lot more series than movies (about three times more). I watched slightly less Netflix in 2020 than in 2019. My Netflix consumption is generally higher around summer but also in October, February and March. I prefer to watch Netflix on my computer when I'm at home but I also use my phone to watch Netflix on my way to work (metro, bus, train, ...). I have lived in France and in England, so you can see that I watch Netflix there the most. However, you can also see that sometimes, when I travel, I also watch a few Netflix.

And last but not least, my tops (which I recommend to watch) :

Series : 
   - How I met your mother (very funny!)
   - Peaky Blinders (very badass!)
   - Breaking Bad (very crazy!)
   - The Crown (very interesting to learn more about the history and context of the UK!)
   - H (very -french-humour- stupidly funny!)
        
Films :
   - The Big Short (A shocking and interesting true event!)
   - I Care a Lot (Music, story, actors,... Everything is perfect in this one!)
   - War dogs (What a crazy story!)
   - Spirited Away (Miyazaki's best movie!).
   - The Dark Knight (A classic!)    
