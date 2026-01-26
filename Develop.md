Chat log: 
https://chatgpt.com/c/6973dc57-4a90-832e-9103-be33a040d25a
https://chatgpt.com/branch/6973dc57-4a90-832e-9103-be33a040d25a/19c73e1b-4573-4322-bdb1-584ea2998d6e
To run locally
func start

To publish to Azure:

`func azure functionapp publish video-annotator-function`

faster, if requirements.txt didn't change:

`func azure functionapp publish video-annotator-function --no-build`

if for some reason this doesn't work, we try:

zip -r app.zip . \
  -x "local.settings.json" ".venv/*" "__pycache__/*" "*.pyc" ".git/*" ".DS_Store"

az functionapp deployment source config-zip \
  --resource-group video-annotator-robot \
  --name video-annotator-function \
  --src app.zip

# logs

az webapp log tail -g video-annotator-robot -n video-annotator-function


