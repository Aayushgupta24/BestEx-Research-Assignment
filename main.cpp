#include <bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <queue>
#include <string>
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;
using namespace std;

// Structure to represent market data
struct MarketData
{
    string symbol;    // Stock symbol (e.g., AAPL, MSFT)
    string timestamp; // Timestamp of the data
    double price;     // Stock price
    int size;         // Number of shares
    string exchange;  // Stock exchange (e.g., NASDAQ)
    string type;      // Transaction type (e.g., BUY, SELL)

    // Comparator for priority queue (min-heap) to sort by timestamp
    bool operator<(const MarketData &other) const
    {
        if (timestamp == other.timestamp)
        {
            return symbol > other.symbol;
        }
        return timestamp > other.timestamp;
    }
};

// Function to parse a line of market data into the MarketData structure
MarketData marketParsingData(const string &line)
{
    istringstream ss(line);
    MarketData data;
    char comma;
    ss >> data.symbol >> comma >> data.timestamp >> comma >> data.price >> comma >> data.size >> comma >> data.exchange >> comma >> data.type;
    return data;
}

// Function to distribute market data into buckets based on the date in the timestamp
void distributeToBuckets(const vector<string> &inputFiles, const string &bucketDir)
{
    fs::create_directory(bucketDir);

    for (const auto &file : inputFiles)
    {
        ifstream inputFile(file);
        if (!inputFile.is_open())
        {
            cerr << "Error opening file: " << file << endl;
            continue;
        }

        string line;
        while (getline(inputFile, line))
        {
            if (line.empty())
                continue;

            MarketData data = marketParsingData(line);
            string date = data.timestamp.substr(0, 10);
            ofstream bucketFile(bucketDir + "/" + date + ".txt", ios::app);
            bucketFile << line << "\n";
        }
    }
}

// Function to sort the data in each bucket file
void sortBuckets(const string &bucketDir)
{
    for (const auto &entry : fs::directory_iterator(bucketDir))
    {
        ifstream bucketFile(entry.path());
        if (!bucketFile.is_open())
        {
            cerr << "Error opening bucket file: " << entry.path() << endl;
            continue;
        }

        vector<MarketData> dataEntries;
        string line;
        while (getline(bucketFile, line))
        {
            if (line.empty())
                continue;
            dataEntries.push_back(marketParsingData(line));
        }
        bucketFile.close();

        sort(dataEntries.begin(), dataEntries.end(), [](const MarketData &a, const MarketData &b)
             {
                 if (a.timestamp == b.timestamp)
                     return a.symbol < b.symbol; 
                 return a.timestamp < b.timestamp; });

        ofstream sortedBucketFile(entry.path());
        for (const auto &data : dataEntries)
        {
            sortedBucketFile << data.symbol << ", " << data.timestamp << ", " << data.price
                             << ", " << data.size << ", " << data.exchange << ", " << data.type << "\n";
        }
    }
}

// Function to merge all sorted bucket files into a single output file
void mergeBuckets(const string &bucketDir, const string &outputFile)
{
    ofstream outFile(outputFile);
    if (!outFile.is_open())
    {
        cerr << "Error opening output file: " << outputFile << endl;
        return;
    }

    priority_queue<MarketData> pq;
    map<string, ifstream> bucketFiles;

    for (const auto &entry : fs::directory_iterator(bucketDir))
    {
        ifstream bucketFile(entry.path());
        if (bucketFile.is_open())
        {
            bucketFiles[entry.path().string()] = move(bucketFile);
            string line;
            if (getline(bucketFiles[entry.path().string()], line))
            {
                pq.push(marketParsingData(line));
            }
        }
    }

    while (!pq.empty())
    {
        MarketData top = pq.top();
        pq.pop();

        outFile << top.symbol << ", " << top.timestamp << ", " << top.price
                << ", " << top.size << ", " << top.exchange << ", " << top.type << "\n";

        for (auto &[path, file] : bucketFiles)
        {
            if (!file.is_open())
                continue;

            string line;
            if (getline(file, line))
            {
                pq.push(marketParsingData(line));
            }
        }
    }

    for (auto &[path, file] : bucketFiles)
    {
        file.close();
    }
}

int main()
{

    fs::create_directory("input");
    fs::create_directory("output");

    vector<string> inputFiles = {"input/file1.txt", "input/file2.txt", "input/file3.txt"};
    string bucketDir = "buckets";
    string outputFile = "output/output.txt";

    // Execute the pipeline
    distributeToBuckets(inputFiles, bucketDir);
    sortBuckets(bucketDir);
    mergeBuckets(bucketDir, outputFile);

    cout << "Processing complete has been output written to " << outputFile << endl;
    return 0;
}
