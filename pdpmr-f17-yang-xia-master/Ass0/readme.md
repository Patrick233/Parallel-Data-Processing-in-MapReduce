---
title: "Sequential VS Concurrent Readme file"
author: "Yang Xia"
date: "9/21/2017"
---

This is the CS6240 Assigment A1 readme file written in RMarkDown.
I wrote a Java class named NeighborhoodScore to take it from there once I had the score for each letter and the string array containing every word in the corpus. I had 2 maps:
one to count the number of occurances for each word, one to sum all k-neighbour score. Once these are done, dived the total sum to number of occurances to get mean score. I didn't introduce concurrency in this phrase since it's hard to split the task into pieces. e.g, say I had "hard to divide workload" at the end of first split, and "into different pieces" at the begining of second split, it becomes complicated to obtain the correct k-neighbour of "into".

I had sequential and concurrent solution in the reading and calculating phrase. The idea is to read all letter at once, get rid of numbers and symbols, change everything to lowercases, and count ouccurances and get a string containing every word. Concurrent solution basically divide books into different chunks and do the same thing for each chunk, sum the occurances and append the string in order. 

