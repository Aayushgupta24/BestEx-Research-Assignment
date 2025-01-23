The assignment is solved using the below approach:
1. In bucket sort, we create buckets based on some value and sort each individual bucket using some other Algo and then merge them together.
2. Here you can create buckets on the basis of date(for example), now suppose you read the first file then you will distribute its entries into the buckets. Now, since you cannot load all the data at once so instead of keeping buckets as variables, we will store each bucket as a file. 
3. Now, finally after processing all the given files we can sort individual buckets and then merge them into a single file.
