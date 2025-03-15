using System;
using System.Data;
using Microsoft.Data.Sql;
using System.IO;
using ExcelDataReader;
using Microsoft.Data.SqlClient;
using static System.Runtime.InteropServices.JavaScript.JSType;

class Program
{
	static void Main(string[] args)
	{
		 
		string employerClassFile = @"C:\Users\Adm\source\repos\DataImporter\Data\EmployerClasses.xls";
		string emplyerSubClassFile = @"C:\Users\Adm\source\repos\DataImporter\Data\EmployerSubClasses.xls";

		 
		string connectionString = "Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=\"C:\\MSSQL database file\\EmployerDBMnelisiMbonani.mdf\";Integrated Security=True;";

		// Register encoding provider (required for ExcelDataReader)
		System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

		try
		{    //Open employerClassFile
			using (var stream = File.Open(employerClassFile, FileMode.Open, FileAccess.Read))
			{
				using (var reader = ExcelReaderFactory.CreateReader(stream))
				{
					var dataSet = reader.AsDataSet();
					var dataTable = dataSet.Tables[0]; // Read first sheet

					using (SqlConnection connection = new SqlConnection(connectionString))
					{
						connection.Open();
						bool isHeaderRow = true;
						foreach (DataRow row in dataTable.Rows)
						{

							if (isHeaderRow)
							{
								isHeaderRow = false; // skip the first row (header)
								continue;
							}
							int EmployerClassRef = Convert.ToInt32(row[0]);
							string EmployerClassType = row[1].ToString();
							string EmployerClassName = row[2].ToString();


							string insertQuery = "INSERT INTO EmployerClasses (EmployerClassRef,EmployerClassType, EmployerClassName) " +
								"VALUES (@EmployerClassRef, @EmployerClassType, @EmployerClassName);";

							using (var command = new SqlCommand(insertQuery, connection))
							{
								command.Parameters.AddWithValue("@EmployerClassRef", EmployerClassRef);
								command.Parameters.AddWithValue("@EmployerClassType", EmployerClassType);
								command.Parameters.AddWithValue("@EmployerClassName", EmployerClassName);

								command.ExecuteNonQuery();
							}
						}
					}
				}
			}

		}
		catch (Exception ex)
		{
			Console.Write(ex.Message);
			return;
		}

		try
		{

			//Open emplyerSubClassFile
			using (var stream = File.Open(emplyerSubClassFile, FileMode.Open, FileAccess.Read))
			{
				using (var reader = ExcelReaderFactory.CreateReader(stream))
				{
					var dataSet = reader.AsDataSet();
					var dataTable = dataSet.Tables[0]; // Read first sheet

					using (SqlConnection connection = new SqlConnection(connectionString))
					{
						connection.Open();
						bool isHeaderRow = true;
						foreach (DataRow row in dataTable.Rows)
						{
							 

							if (isHeaderRow)
							{
								isHeaderRow = false;  
							}

							int EmployerSubclassCode = Convert.ToInt32(row[1]);
							int ClassRef = Convert.ToInt32(row[0]);
							string EmployerSubclassDescription = row[2].ToString();
							string EmployerSubclassShortDescription = row[3].ToString();

							//  Check if ClassRef exists in EmployerClasses
							string checkQuery = "SELECT COUNT(*) FROM EmployerClasses WHERE EmployerClassRef = @ClassRef";
							using (var checkCommand = new SqlCommand(checkQuery, connection))
							{
								checkCommand.Parameters.AddWithValue("@ClassRef", ClassRef);
								int count = (int)checkCommand.ExecuteScalar();

								if (count == 0)
								{
									 
									Console.WriteLine($"Skipping record - ClassRef {ClassRef} does not exist in EmployerClasses.");
									continue; // Skip this row if the class doesn't exist
								}
							}

							 
							string insertQuery = "IF NOT EXISTS (SELECT 1 FROM EmployerSubClasses WHERE EmployerSubclassCode = @EmployerSubclassCode )" + 
                         "BEGIN INSERT INTO EmployerSubClasses(EmployerSubclassCode, ClassRef, EmployerSubclassDescription, EmployerSubclassShortDescription)" +
                         "VALUES (@EmployerSubclassCode, @ClassRef,@EmployerSubclassDescription, @EmployerSubclassShortDescription) END;";

							using (var command = new SqlCommand(insertQuery, connection))
							{
								command.Parameters.AddWithValue("@EmployerSubclassCode", EmployerSubclassCode);
								command.Parameters.AddWithValue("@ClassRef", ClassRef);
								command.Parameters.AddWithValue("@EmployerSubclassDescription", EmployerSubclassDescription);
								command.Parameters.AddWithValue("@EmployerSubclassShortDescription", EmployerSubclassShortDescription);

								command.ExecuteNonQuery();
							}
						}
					}
				}
			}

		}
		catch (Exception ex)
		{
			Console.Write(ex.Message);
			return;
		}


		Console.WriteLine("Data imported successfully!");
		Console.ReadKey();
	}
}
