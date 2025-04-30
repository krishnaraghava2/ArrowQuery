# Build the Rust library
Write-Host "Building Rust library..."
Push-Location "arrow_query"
cargo build --release
Pop-Location

# Copy the built DLL to the .NET runtimes directory
$rustDll = "arrow_query\target\release\arrow_query.dll"
$interopNativeDir = "ArrowQuery.Interop\runtimes\win-x64\native"
if (Test-Path $rustDll) {
    Write-Host "Copying Rust DLL to .NET native directory..."
    Copy-Item $rustDll -Destination $interopNativeDir -Force
} else {
    Write-Host "Rust DLL not found. Build may have failed."
    exit 1
}

# Build and pack the .NET interop project
Write-Host "Packing ArrowQuery.Interop NuGet package..."
Push-Location "ArrowQuery.Interop"
dotnet pack -c Release
Pop-Location

Write-Host "Pipeline completed."
