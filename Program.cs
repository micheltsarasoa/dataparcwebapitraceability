/*
 * [Your Project Name]
 * Copyright (C) [Year] [Your Name/Organization]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

using System;
using System.Runtime.InteropServices;
using WebServiceTracability;

public class Program
{
    private static void Main(string[] args)
    {
        // Create a new WebApplicationBuilder instance
        var builder = WebApplication.CreateBuilder(args);

        // Clear default logging providers and add custom providers
        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();
        builder.Logging.AddDebug();

        // Check if the operating system is Windows
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // Configure logging to Windows Event Viewer
            builder.Logging.AddEventLog(settings =>
            {
                settings.SourceName = "Traceability Web Api";

            });
        }
        // Add services to the container.
        // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
        builder.Services.AddControllers(); // Add support for controllers
        builder.Services.AddEndpointsApiExplorer(); // Add support for API exploration
        builder.Services.AddSwaggerGen(); // Add support for Swagger documentation

        // Get Settings
        builder.Services.Configure<AppSettings>(builder.Configuration.GetSection("AppSettings"));

        // Build the WebApplication instance
        var app = builder.Build();

        // Configure the HTTP request pipeline.
        if (app.Environment.IsDevelopment()) // If the application is running in development mode
        {
            app.UseSwagger();   // Enable Swagger documentation
            app.UseSwaggerUI(); // Enable the Swagger UI
        }

        // app.UseHttpsRedirection(); // Redirect HTTP requests to HTTPS
        app.MapControllers(); // Map controllers to the request pipeline

        // Run the application
        app.Run();
    }
}