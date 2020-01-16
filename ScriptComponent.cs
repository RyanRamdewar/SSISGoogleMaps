#region Help:  Introduction to the Script Component
/* The Script Component allows you to perform virtually any operation that can be accomplished in
 * a .Net application within the context of an Integration Services data flow.
 *
 * Expand the other regions which have "Help" prefixes for examples of specific ways to use
 * Integration Services features within this script component. */
#endregion

#region Namespaces
using System;
using System.Data;
using System.IO;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
#endregion

/// <summary>
/// This is the class to which to add your code.  Do not change the name, attributes, or parent
/// of this class.
/// </summary>
[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    #region Help:  Using Integration Services variables and parameters
    /* To use a variable in this script, first ensure that the variable has been added to
     * either the list contained in the ReadOnlyVariables property or the list contained in
     * the ReadWriteVariables property of this script component, according to whether or not your
     * code needs to write into the variable.  To do so, save this script, close this instance of
     * Visual Studio, and update the ReadOnlyVariables and ReadWriteVariables properties in the
     * Script Transformation Editor window.
     * To use a parameter in this script, follow the same steps. Parameters are always read-only.
     *
     * Example of reading from a variable or parameter:
     *  DateTime startTime = Variables.MyStartTime;
     *
     * Example of writing to a variable:
     *  Variables.myStringVariable = "new value";
     */
    #endregion

    #region Help:  Using Integration Services Connnection Managers
    /* Some types of connection managers can be used in this script component.  See the help topic
     * "Working with Connection Managers Programatically" for details.
     *
     * To use a connection manager in this script, first ensure that the connection manager has
     * been added to either the list of connection managers on the Connection Managers page of the
     * script component editor.  To add the connection manager, save this script, close this instance of
     * Visual Studio, and add the Connection Manager to the list.
     *
     * If the component needs to hold a connection open while processing rows, override the
     * AcquireConnections and ReleaseConnections methods.
     * 
     * Example of using an ADO.Net connection manager to acquire a SqlConnection:
     *  object rawConnection = Connections.SalesDB.AcquireConnection(transaction);
     *  SqlConnection salesDBConn = (SqlConnection)rawConnection;
     *
     * Example of using a File connection manager to acquire a file path:
     *  object rawConnection = Connections.Prices_zip.AcquireConnection(transaction);
     *  string filePath = (string)rawConnection;
     *
     * Example of releasing a connection manager:
     *  Connections.SalesDB.ReleaseConnection(rawConnection);
     */
    #endregion

    #region Help:  Firing Integration Services Events
    /* This script component can fire events.
     *
     * Example of firing an error event:
     *  ComponentMetaData.FireError(10, "Process Values", "Bad value", "", 0, out cancel);
     *
     * Example of firing an information event:
     *  ComponentMetaData.FireInformation(10, "Process Values", "Processing has started", "", 0, fireAgain);
     *
     * Example of firing a warning event:
     *  ComponentMetaData.FireWarning(10, "Process Values", "No rows were received", "", 0);
     */
    #endregion

    /// <summary>
    /// This method is called once for every row that passes through the component from Input0.
    ///
    /// Example of reading a value from a column in the the row:
    ///  string zipCode = Row.ZipCode
    ///
    /// Example of writing a value to a column in the row:
    ///  Row.ZipCode = zipCode
    /// </summary>
    /// <param name="Row">The row that is currently passing through the component</param>
    public override void Input0_ProcessInputRow(Input0Buffer Row)
    {
        string place_id = null;
        try
        {
            //if (Row.AccountGUID == new Guid("826EA96C-ABD3-E811-A96D-000D3AFF2E34"))
            //{
            //    place_id = null;
            //}

            // check for full establishment name and address
            var xmlDoc = new XmlDocument();
            string url = "https://maps.googleapis.com/maps/api/geocode/xml?address={0}&fields=&key={1}";
            string address = Regex.Replace(Row.FullInternalAddress, @"['\/~`\!@#\$%\^&\*\(\)_\-\+=\{\}\[\]\|;:""\<\>,\.\?\\]", "");
            var wGet = WebRequest.Create(String.Format(url, address, Variables.webApiKey));
            wGet.Method = WebRequestMethods.Http.Get;
            var response = wGet.GetResponse();
            var status = ((HttpWebResponse)response).StatusDescription;
            if (status == "OK")
            {
                ReadResponse(Row, xmlDoc, response, true, out place_id);
                response.Close();
            }

            // check only address
            if (place_id == null)
            {
                xmlDoc = new XmlDocument();
                url = "https://maps.googleapis.com/maps/api/geocode/xml?address={0}&key={1}";
                address = Row.InternalAddress1 + " " + Row.InternalAddress2 + " " + Row.InternalCity + " " + Row.InternalPostalCode;
                address = Regex.Replace(address, @"['\/~`\!@#\$%\^&\*\(\)_\-\+=\{\}\[\]\|;:""\<\>,\.\?\\]", "");
                wGet = WebRequest.Create(String.Format(url, address, Variables.webApiKey));
                wGet.Method = WebRequestMethods.Http.Get;
                response = wGet.GetResponse();
                status = ((HttpWebResponse)response).StatusDescription;

                if (status == "OK")
                {
                    ReadResponse(Row, xmlDoc, response, false, out place_id);
                    response.Close();
                }
            }

            // mark error if no place found
            if (place_id == null || place_id.Trim() == "")
            {
                Row.GoogleID = "error";
            }
        }
        catch (Exception ex)
        {
            this.ComponentMetaData.FireWarning(0, "Goggle Lookup", "Error message: " + ex.Message, String.Empty, 0);

        }
        // google limits 50 requests per second
        Thread.Sleep(20);
    }

    private void ReadResponse(Input0Buffer Row, XmlDocument xmlDoc, WebResponse response, bool getDetails, out string place_id)
    {
        place_id = null;
        // Open the stream using a StreamReader for easy access.
        StreamReader reader = new StreamReader(response.GetResponseStream());

        // Read the content fully up to the end.
        string responseFromServer = reader.ReadToEnd();
        // Clean up the streams.
        reader.Close();

        xmlDoc.LoadXml(responseFromServer);

        string sts = xmlDoc.DocumentElement.SelectSingleNode("status").InnerText;
        if (sts != "ZERO_RESULTS")
        {
            try
            {
                place_id = xmlDoc.DocumentElement.SelectSingleNode("/GeocodeResponse/result/place_id").InnerText;

                if (place_id != null)
                {
                    string streetNumber = null;
                    string streetName = null;
                    string subpremise = null;
                    string city = null;
                    string province = null;
                    string country = null;
                    string postal = null;
                    string lat = null;
                    string lng = null;
                    string icon = null;
                    string phone;
                    string webUrl;
                    string name = "Internal Address";

                    var address = xmlDoc.DocumentElement.SelectNodes("/GeocodeResponse/result/address_component");
                    foreach (XmlNode node in address)
                    {
                        string type = node.SelectSingleNode("type").InnerText;
                        switch (type)
                        {
                            case "street_number":
                                streetNumber = node.SelectSingleNode("long_name").InnerText;
                                break;
                            case "subpremise":
                                subpremise = node.SelectSingleNode("long_name").InnerText;
                                break;
                            case "route":
                                streetName = node.SelectSingleNode("long_name").InnerText;
                                break;
                            case "locality":
                                city = node.SelectSingleNode("long_name").InnerText;
                                break;
                            case "administrative_area_level_1":
                                province = node.SelectSingleNode("short_name").InnerText;
                                break;
                            case "country":
                                country = node.SelectSingleNode("long_name").InnerText;
                                break;
                            case "postal_code":
                                postal = node.SelectSingleNode("long_name").InnerText;
                                break;
                        }
                    }
                    lat = xmlDoc.DocumentElement.SelectSingleNode("/GeocodeResponse/result/geometry/location/lat").InnerText;
                    lng = xmlDoc.DocumentElement.SelectSingleNode("/GeocodeResponse/result/geometry/location/lng").InnerText;

                    getPlaceDetails(place_id, out name, out phone, out webUrl, out icon);
                    if (getDetails)
                    {
                        Row.Phone = phone;
                        Row.WebSite = webUrl;
                    }

                    Row.Lat = decimal.Parse(lat);
                    Row.Lng = decimal.Parse(lng);
                    Row.Address1 = streetNumber + " " + streetName;
                    if (subpremise != null)
                    {
                        Row.Address2 = "#" + subpremise;
                    }
                    Row.City = city;
                    Row.Province = province;
                    Row.Country = country;
                    Row.Postal = postal;
                    Row.Name = name;
                    Row.Icon = icon;
                                       
                    Row.GoogleID = place_id;
                }
            }
            catch (Exception ex)
            {
                bool pbCancel = false;
                this.ComponentMetaData.FireError(0, "Goggle Lookup[CRM_ID=" + Row.AccountGUID + "]", "Error message: " + ex.Message + "|  " + ex.InnerException.Message, String.Empty, 0, out pbCancel);

            }
        }
    }

    private void getPlaceDetails(string placeID, out string name, out string phone, out string webUrl, out string icon)
    {
        name = null;
        phone = null;
        webUrl = null;
        icon = null;
        var xmlDoc = new XmlDocument();
        string url = "https://maps.googleapis.com/maps/api/place/details/xml?place_id={0}&fields=name,website,formatted_phone_number,icon&key={1}";
        var wGet = WebRequest.Create(String.Format(url, placeID, Variables.webApiKey));
        wGet.Method = WebRequestMethods.Http.Get;
        var response = wGet.GetResponse();
        var status = ((HttpWebResponse)response).StatusDescription;

        if (status == "OK")
        {
            // Open the stream using a StreamReader for easy access.
            StreamReader reader = new StreamReader(response.GetResponseStream());

            // Read the content fully up to the end.
            string responseFromServer = reader.ReadToEnd();
            // Clean up the streams.
            reader.Close();

            xmlDoc.LoadXml(responseFromServer);

            name = xmlDoc.DocumentElement.SelectSingleNode("/PlaceDetailsResponse/result/name") != null ? xmlDoc.DocumentElement.SelectSingleNode("/PlaceDetailsResponse/result/name").InnerText : null;
            phone = xmlDoc.DocumentElement.SelectSingleNode("/PlaceDetailsResponse/result/formatted_phone_number") != null ? xmlDoc.DocumentElement.SelectSingleNode("/PlaceDetailsResponse/result/formatted_phone_number").InnerText : null;
            webUrl = xmlDoc.DocumentElement.SelectSingleNode("/PlaceDetailsResponse/result/website") != null ? xmlDoc.DocumentElement.SelectSingleNode("/PlaceDetailsResponse/result/website").InnerText : null;
            icon = xmlDoc.DocumentElement.SelectSingleNode("/PlaceDetailsResponse/result/icon") != null ? xmlDoc.DocumentElement.SelectSingleNode("/PlaceDetailsResponse/result/icon").InnerText : null;
        }
        response.Close();

    }


}
