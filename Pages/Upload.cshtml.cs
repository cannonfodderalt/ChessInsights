using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;

public class IndexModel : PageModel
{
    private readonly IConfiguration _configuration;
    private readonly string _containerName = "chessdatastorage";

    public IndexModel(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public void OnGet() { }

    public async Task<IActionResult> OnPostUploadAsync(IFormFile pgnFile)
    {
        if (pgnFile != null && pgnFile.Length > 0)
        {
            string blobConnectionString = _configuration.GetConnectionString("AzureBlobStorage") ?? "Empty";

            var blobServiceClient = new BlobServiceClient(blobConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(_containerName);

            string uniqueFileName = $"{Guid.NewGuid()}_{pgnFile.FileName}";
            var blobClient = containerClient.GetBlobClient(uniqueFileName);

            using (var stream = pgnFile.OpenReadStream())
            {
                await blobClient.UploadAsync(stream, true);
            }

            TempData["Message"] = "Success!";
        }

        return RedirectToPage();
    }
}