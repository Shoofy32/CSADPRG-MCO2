/********************
Last names: Domingo, Copon, Sarroza, Tiongco
Language: Kotlin
Paradigm(s): Functional Programming
********************/

/*
List of imports, this project relies heavily on the Gradle framework and the
importing of the external kotlinx library to use dataframes which allows the
program to be able to manipulate and read the csv file data
 */
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.*
import org.jetbrains.kotlinx.dataframe.io.readCSV
import org.jetbrains.kotlinx.dataframe.io.writeCSV
import kotlinx.datetime.*
import org.jetbrains.kotlinx.dataframe.io.writeJson
import kotlin.math.round
/*
To import Dataframes the "build.gradle.kts" file's dependencies and plugins had to be modified in the following:
dependencies {
    testImplementation(kotlin("test"))
    implementation("org.jetbrains.kotlinx:dataframe:0.13.1") <- added this to the list of dependencies
}

plugins {
    kotlin("jvm") version "2.1.21"
    kotlin("plugin.dataframe") version "2.2.20" <- added this to the list of plugins
}
Additionally, the "gradle.properties" file also had to be changed:
kotlin.code.style=official
kotlin.incremental=false <- added

The guide for this setup is linked below
https://kotlin.github.io/dataframe/setupgradle.html#next-steps
 */
private var exists = false

fun main() {
    //dummy data frame to initialize dataframe variable
    var df = dataFrameOf("Dummy" to listOf(1))
    var choice = "0"
    while (choice != "3" ){
        println("[1] Load and process file")
        println("[2] Generate reports")
        println("[3] End program")
        print("Please choose an option to proceed: ")
        choice = readln()

        when (choice) {
            "1" if !exists -> {
                df = clean()
            }
            "2" if exists -> {
                print("\n\n")
                reportOne(df)
                print("\n\n")
                reportTwo(df)
                print("\n\n")
                reportThree(df)
                print("\n\n")
                summary(df)
            }
            "1" if exists -> {
                println("File has already been loaded and cleaned")
            }
            "3" -> {
                println("Program will now end")
            }
            else -> {
                println("That is not a valid option")
            }
        }
    }

}

fun clean (): DataFrame<*> {
    //Reads csv file
    var df = DataFrame.readCSV("floods.csv")
    println("${df.rowsCount()} rows read, proceeding to data cleaning...")

    //drops empty rows or rows with missing data
    df = df.dropNA()

    //filters out projects containing start and/or end dates falling outside 2021-2023
    //casts StartDate column data into Date data type for comparison
    df = df.convert("StartDate") { it.toString().toLocalDate() }
    df = df.filter {
        val date = it["StartDate"] as LocalDate
        date.year in 2021..2023
    }

    /*df = df.convert("ActualCompletionDate") { it.toString().toLocalDate() }
    df = df.filter {
        val date = it["ActualCompletionDate"] as LocalDate
        date.year in 2021..2023
    }*/

    /*Converts all rows wherein the project cost was clustered with another project to 0
    Ex. ProjectId P00726656LZ had its cost clustered with P00726404LZ meaning the latter's budget
    encompasses the former, setting the cost to 0 for the former retains the project's identity in the data
    but also reflects it's actual cost.*/
    df = df.convert { "ApprovedBudgetForContract"<String>() }.with {it.toDoubleOrNull() ?: 0}

    //Adds in the CostSavings and Completion Delay Days columns
    df = df.convert{"ApprovedBudgetForContract"<Any>()}.toDouble()
    df = df.convert{"ContractCost"<String>()}.toDouble()
    df = df.add("CostSavings") {it["ApprovedBudgetForContract"] as Double - it["ContractCost"] as Double}
    df = df.add("CompletionDelayDays") {
        val start = it["StartDate"] as LocalDate
        val comp = it["ActualCompletionDate"] as LocalDate
        start.daysUntil(comp)
    }
    df = df.convert("CostSavings").toDouble()
    println("${df.rowsCount()} rows loaded, all dirty data has been removed")
    exists = true
    return df
}

fun reportOne (df: DataFrame<*>) {
    var df = df.groupBy ("Region","MainIsland").aggregate{
        val agg = sum("ApprovedBudgetForContract")
        //formats computed number into having only two decimal places
        String.format("PHP %.2f",agg) into "Budget Total"
        val medCS = median("CostSavings")
        String.format("%.2f",medCS) into "Median Cost Savings"
        val daydelay = mean("CompletionDelayDays")
        String.format("%.2f",daydelay) into "Average Completion Delay Days"
        //stores a total count of projects
        val total = count()
        //counts the projects with a delay greater than 30 days
        val delproj = count { ((it["CompletionDelayDays"] as? Number)?.toDouble() ?: 0.0) > 30.0 }
        //computes the percentage
        String.format("%.2f%%", (delproj.toDouble() / total.toDouble() * 100)) into "HighDelayPct"
    }
    //converts both columns into doubles for computing efficiency score
    df = df.convert{"Median Cost Savings"<String>()}.toDouble()
    df = df.convert{"Average Completion Delay Days"<String>()}.toDouble()

    //this column will be temporary, used to compute raw efficiency scores for normalization
    //elvis operator to force the use of a specific value if the left value is inferred as null
    df = df.add("esraw") {
        val medCS = (it["Median Cost Savings"] as? Number)?.toDouble() ?: 0.0
        val daydelay = (it["Average Completion Delay Days"] as? Number)?.toDouble() ?: 1.0
        medCS / daydelay * 100
    }

    //storing max and min values of raw efficiency scores to normalize them
    val esmin = df["esraw"].cast<Double>().min()
    val esmax = df["esraw"].cast<Double>().max()

    //normalizes efficiency score
    df = df.add("Efficiency Score"){
        val raw = it["esraw"] as Double
        round((((raw - esmin) / (esmax - raw) * 100)) * 100) / 100
    }

    //caps infinity values to 100 efficiency score
    df = df.update("Efficiency Score").with {
        //cast value as double? to be able to use ".isInfinite" method
        val v = (it as? Number)?.toDouble() ?: 0.0
        if (v.isInfinite()) 100.0 else v
    }

    //converts Median Cost Savings column into financial number
    df = df.convert("Median Cost Savings") {
        String.format("PHP %.2f", it)
    }

    //removes "esraw"
    df = df.remove("esraw")

    //orders by efficiency score
    df = df.sortByDesc("Efficiency Score")


    println("Displaying first 5 rows...")
    //prints first 5 rows
    df.print(5)

    println("Data outputted to Report_1_Regional_Flood_Mitigation_Efficiency_Summary.csv")

    //prints report
    df.writeCSV("Report_1_Regional_Flood_Mitigation_Efficiency_Summary.csv")
}

fun reportTwo (df: DataFrame<*>) {
    var df = df.groupBy("Contractor").aggregate {
        val sum = sum("ContractCost")
        round(sum.toDouble() * 100) / 100 into "Total Contract Cost"
        //counting projects per contractor
        count() into "No of Projects"
        val daydelay = mean("CompletionDelayDays")
        round(daydelay * 100) / 100 into "Avg Delay Days"
        val agg = sum("CostSavings")
        round(agg.toDouble() * 100) / 100 into "Total Cost Savings"
    }

    //dropping contractors with less than 5 projects
    df.convert("No of Projects").toInt()
    df = df.filter { it["No of Projects"] as Int >= 5 }

    //computing for reliability index
    df.convert("Avg Delay Days").toDouble()
    df.convert("Total Cost Savings").toDouble()
    df.convert("Total Contract Cost").toDouble()
    df = df.add("Reliability Index") {
        val delay = it["Avg Delay Days"] as Double
        val totCS = it["Total Cost Savings"] as Double
        val totC = it["Total Contract Cost"] as Double

        val ri = (1 - (delay / 90.0)) * (totCS / totC) * 100.0

        //capping values to 100
        if (ri > 100.0) 100.0 else round(ri * 100.0) / 100.0
    }


    //adding in risk flag based on reliability index
    df = df.add("Risk Flag") {
        val ri = it["Reliability Index"] as Double
        if (ri < 50.0) "High Risk" else "Low Risk"
    }

    df = df.convert("Total Cost Savings"){
        String.format("PHP %.2f", it)
    }

    df = df.sortByDesc("Total Contract Cost")

    df = df.convert("Total Contract Cost") {
        String.format("PHP %.2f", it)
    }

    println("Displaying first 5 rows...")
    df.print(5)

    //limits only by the top 15 Contractors
    df = df.head(15)

    println("Data outputted to Report_2_Top_Contractors_Performance_Ranking.csv")
    df.writeCSV("Report_2_Top_Contractors_Performance_Ranking.csv")
}

fun reportThree (df: DataFrame<*>) {
    var df = df.groupBy("FundingYear", "TypeOfWork").aggregate {
        val total = count()
        total into "Total Projects"
        val avgCS = mean("CostSavings")
        round(avgCS * 100) / 100 into "AvgSavings"
        val ovr = count { ((it["CostSavings"] as? Number)?.toDouble() ?: 0.0) < 0.0 }
        String.format("%.2f%%", (ovr.toDouble() / total.toDouble() * 100)) into "Overrun Rate"
    }

    df = df.sortBy("FundingYear")

    df = df.add("Year Over Year Change") {
        //storing the index of current row
        val index = index()

        if(index == 0) { "PHP 0.0" }
        else {
            //getting the value of the previous row
            val prev = df[index - 1]["AvgSavings"] as Double
            val cur = it["AvgSavings"] as Double
            if (prev == 0.0) "PHP 0.0" else String.format("PHP %.2f", ((cur - prev) / prev) * 100)
        }
    }

    //sorting by descending average savings only since dataframe has already been sorted by year ascendingly
    df = df.sortByDesc("AvgSavings")

    df = df.convert("AvgSavings"){ String.format("PHP %.2f", it) }

    println("Displaying first 5 rows...")
    df.print(5)

    println("Data outputted to Report_3_Annual_Project_Type_Cost_Overrun_Trends.csv")
    df.writeCSV("Report_3_Annual_Project_Type_Cost_Overrun_Trends.csv")
}

fun summary (df: DataFrame<*>) {

    //creating a single row dataframe containing all the summary statistics
    var dsum = dataFrameOf(
        "Total No of Projects" to listOf(df.countDistinct("ProjectId")),
        "Total Contractors" to listOf(df.countDistinct("Contractor")),
        "Total Provinces with Projects" to listOf(df.countDistinct("Province")),
        "Global Average Delay" to listOf(df.mean("CompletionDelayDays")),
        "Total Savings" to listOf(df.sum("CostSavings"))
    )

    //formatting columns
    dsum = dsum.convert("Global Average Delay") {String.format("%.2f", it)}
    dsum = dsum.convert("Total Savings") {String.format("PHP %.2f", it)}

    println("Displaying summary statistics")
    dsum.print()

    //writing to json file
    println("Data saved to summary.json")
    dsum.writeJson("summary.json")
}
